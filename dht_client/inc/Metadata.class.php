<?php

/**
 * Metadata 下载类
 * 基于 BEP 0009 (Extension for Peers to Send Metadata Files) 实现
 * 同时适配 Swoole\Coroutine\Client（协程客户端）和 Swoole\Client（同步客户端）
 *
 * 重构为实例化：每个下载任务拥有独立的读缓冲区，支持同进程内多协程并发下载
 */
class Metadata
{
    public static $_bt_protocol = 'BitTorrent protocol';
    public static $BT_MSG_ID = 20;
    public static $EXT_HANDSHAKE_ID = 0;
    public static $PIECE_LENGTH = 16384;

    /** @var string 实例级读缓冲区（协程模式使用，每个下载任务独立） */
    private $buffer = '';

    /** @var bool 是否为协程客户端 */
    private $isCoroutine = false;

    /**
     * 下载种子元数据（静态入口，兼容旧调用方式）
     * @param Swoole\Client|Swoole\Coroutine\Client $client 已连接的TCP客户端
     * @param string $infohash 20字节的infohash
     * @return array|false 成功返回元数据数组，失败返回false
     */
    public static function download_metadata($client, $infohash)
    {
        $instance = new self($client);
        return $instance->doDownload($client, $infohash);
    }

    /**
     * 构造函数，根据客户端类型初始化
     * @param Swoole\Client|Swoole\Coroutine\Client $client
     */
    public function __construct($client)
    {
        $this->isCoroutine = ($client instanceof Swoole\Coroutine\Client);
        $this->buffer = '';
    }

    /**
     * 执行下载
     * @param Swoole\Client|Swoole\Coroutine\Client $client
     * @param string $infohash
     * @return array|false
     */
    private function doDownload($client, $infohash)
    {
        $result = false;
        $_data = [];

        try {
            $start_time = microtime(true);
            $total_timeout = 30;

            // 1. BT握手
            $packet = $this->send_handshake($client, $infohash);
            if ($packet === false || microtime(true) - $start_time > $total_timeout) {
                return false;
            }

            if (!$this->check_handshake($packet, $infohash)) {
                return false;
            }

            // 2. 扩展握手
            $packet = $this->send_ext_handshake($client);
            if ($packet === false || microtime(true) - $start_time > $total_timeout) {
                return false;
            }

            // 3. 解析扩展握手响应，获取ut_metadata id和metadata_size
            $ext_info = $this->parse_ext_handshake($packet);
            if ($ext_info === false) {
                return false;
            }

            $ut_metadata = $ext_info['ut_metadata'];
            $metadata_size = $ext_info['metadata_size'];

            if ($ut_metadata <= 0) {
                return false;
            }

            // 安全限制
            if ($metadata_size > self::$PIECE_LENGTH * 256 || $metadata_size < 10) {
                return false;
            }

            $piecesNum = ceil($metadata_size / self::$PIECE_LENGTH);
            if ($piecesNum > 256) {
                return false;
            }

            // 4. 滑动窗口请求元数据
            $received_pieces = [];
            $next_piece = 0;
            $window_size = 8;
            $inflight = 0;

            // 初始填充窗口
            while ($next_piece < $piecesNum && $inflight < $window_size) {
                if (!$this->request_metadata($client, $ut_metadata, $next_piece)) {
                    return false;
                }
                $next_piece++;
                $inflight++;
            }

            while (count($received_pieces) < $piecesNum) {
                if (microtime(true) - $start_time > $total_timeout) {
                    return false;
                }

                $packet = $this->recvall($client);
                if ($packet === false) {
                    return false;
                }

                while (strlen($packet) > 0) {
                    $piece_result = $this->parse_metadata_piece($packet);
                    if ($piece_result === false) {
                        break;
                    }

                    if ($piece_result['msg_type'] == 1 && isset($piece_result['piece'])) {
                        $piece_index = $piece_result['piece'];
                        if (!isset($received_pieces[$piece_index]) && $piece_index < $piecesNum) {
                            $received_pieces[$piece_index] = $piece_result['data'];
                            $inflight--;
                            while ($next_piece < $piecesNum && $inflight < $window_size) {
                                if (!$this->request_metadata($client, $ut_metadata, $next_piece)) {
                                    return false;
                                }
                                $next_piece++;
                                $inflight++;
                            }
                        }
                    } elseif ($piece_result['msg_type'] == 2) {
                        return false;
                    }

                    $packet = $piece_result['remaining'];
                }
            }

            if (microtime(true) - $start_time > $total_timeout) {
                return false;
            }

            // 5. 组装元数据
            $metadata_str = '';
            for ($i = 0; $i < $piecesNum; $i++) {
                if (!isset($received_pieces[$i])) {
                    return false;
                }
                $metadata_str .= $received_pieces[$i];
            }
            unset($received_pieces);

            $metadata_decoded = Base::decode($metadata_str);
            unset($metadata_str);

            if (!is_array($metadata_decoded)) {
                return false;
            }

            $_infohash = strtoupper(bin2hex($infohash));
            if (isset($metadata_decoded['name']) && $metadata_decoded['name'] != '') {
                $_data['name'] = Func::characet($metadata_decoded['name']);
                $_data['infohash'] = $_infohash;
                $_data['files'] = isset($metadata_decoded['files']) ? $metadata_decoded['files'] : '';
                $_data['length'] = isset($metadata_decoded['length']) ? $metadata_decoded['length'] : 0;
                $_data['piece_length'] = isset($metadata_decoded['piece length']) ? $metadata_decoded['piece length'] : 0;

                $result = $_data;
            }
        } catch (Throwable $e) {
            error_log("Metadata download error: " . $e->getMessage());
            $result = false;
        }

        return $result;
    }

    /**
     * 发送元数据piece请求
     */
    private function request_metadata($client, $ut_metadata, $piece)
    {
        $msg = chr(self::$BT_MSG_ID) . chr($ut_metadata) . Base::encode(array("msg_type" => 0, "piece" => $piece));
        $msg_len = pack("N", strlen($msg));
        $_msg = $msg_len . $msg;

        $rs = $client->send($_msg);
        return $rs !== false;
    }

    /**
     * 精确读取指定字节数
     */
    private function recvExact($client, $length)
    {
        if (!$this->isCoroutine) {
            return $client->recv($length, true);
        }

        while (strlen($this->buffer) < $length) {
            if (!$this->fillBuffer($client)) {
                return false;
            }
        }

        $result = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);
        return $result;
    }

    /**
     * 从网络填充内部缓冲区（仅协程模式）
     */
    private function fillBuffer($client, $timeout = 1.0)
    {
        $data = $client->recv($timeout);
        if ($data === false || $data === '') {
            return false;
        }
        $this->buffer .= $data;
        return true;
    }

    /**
     * 接收完整的BT消息（4字节长度前缀 + 负载），自动跳过 keep-alive
     */
    private function recvall($client)
    {
        if (!$this->isCoroutine) {
            while (true) {
                $header = $client->recv(4, true);
                if ($header === false || strlen($header) < 4) {
                    return false;
                }

                $length = unpack('N', $header)[1];

                if ($length == 0) {
                    continue;
                }

                if ($length > 16777216) {
                    return false;
                }

                $payload = $client->recv($length, true);
                if ($payload === false || strlen($payload) < $length) {
                    return false;
                }

                return $payload;
            }
        }

        // 协程客户端模式
        while (true) {
            if (strlen($this->buffer) < 4) {
                if (!$this->fillBuffer($client)) {
                    return false;
                }
                continue;
            }

            $length = unpack('N', substr($this->buffer, 0, 4))[1];

            if ($length == 0) {
                $this->buffer = substr($this->buffer, 4);
                continue;
            }

            if ($length > 16777216) {
                return false;
            }

            $total = 4 + $length;
            while (strlen($this->buffer) < $total) {
                if (!$this->fillBuffer($client)) {
                    return false;
                }
            }

            $result = substr($this->buffer, 4, $length);
            $this->buffer = substr($this->buffer, $total);
            return $result;
        }
    }

    /**
     * 发送BT协议握手
     */
    private function send_handshake($client, $infohash)
    {
        $bt_protocol = self::$_bt_protocol;
        $bt_header = chr(strlen($bt_protocol)) . $bt_protocol;
        $ext_bytes = "\x00\x00\x00\x00\x00\x10\x00\x00";
        $peer_id = Base::get_node_id();
        $packet = $bt_header . $ext_bytes . $infohash . $peer_id;

        $rs = $client->send($packet);
        if ($rs === false) {
            return false;
        }

        $data = $this->recvExact($client, 68);
        if ($data === false || strlen($data) < 68) {
            return false;
        }

        return $data;
    }

    /**
     * 验证BT握手响应
     */
    private function check_handshake($packet, $self_infohash)
    {
        if (strlen($packet) < 68) {
            return false;
        }

        $bt_header_len = ord($packet[0]);
        if ($bt_header_len != strlen(self::$_bt_protocol)) {
            return false;
        }

        $bt_header = substr($packet, 1, $bt_header_len);
        if ($bt_header !== self::$_bt_protocol) {
            return false;
        }

        $reserved = substr($packet, 1 + $bt_header_len, 8);
        if ((ord($reserved[5]) & 0x10) === 0) {
            return false;
        }

        $infohash = substr($packet, 1 + $bt_header_len + 8, 20);
        if ($infohash !== $self_infohash) {
            return false;
        }

        return true;
    }

    /**
     * 发送扩展握手
     */
    private function send_ext_handshake($client)
    {
        $msg = chr(self::$BT_MSG_ID) . chr(self::$EXT_HANDSHAKE_ID) . Base::encode(array("m" => array("ut_metadata" => 1)));
        $msg_len = pack("N", strlen($msg));
        $msg = $msg_len . $msg;

        $rs = $client->send($msg);
        if ($rs === false) {
            return false;
        }

        $data = $this->recvall($client);
        if ($data === false || $data === '') {
            return false;
        }

        return $data;
    }

    /**
     * 解析扩展握手响应
     */
    private function parse_ext_handshake($data)
    {
        if (strlen($data) < 2) {
            return false;
        }

        $msg_id = ord($data[0]);
        $ext_id = ord($data[1]);

        if ($msg_id != self::$BT_MSG_ID || $ext_id != self::$EXT_HANDSHAKE_ID) {
            return false;
        }

        $bencoded = substr($data, 2);
        $dict = Base::decode($bencoded);

        if (!is_array($dict)) {
            return false;
        }

        $ut_metadata = 0;
        if (isset($dict['m']) && is_array($dict['m']) && isset($dict['m']['ut_metadata'])) {
            $ut_metadata = intval($dict['m']['ut_metadata']);
        }

        $metadata_size = 0;
        if (isset($dict['metadata_size'])) {
            $metadata_size = intval($dict['metadata_size']);
        }

        if ($ut_metadata <= 0 || $metadata_size <= 0) {
            return false;
        }

        return ['ut_metadata' => $ut_metadata, 'metadata_size' => $metadata_size];
    }

    /**
     * 解析元数据piece响应
     */
    private function parse_metadata_piece(&$packet)
    {
        if (strlen($packet) < 2) {
            return false;
        }

        $msg_id = ord($packet[0]);
        if ($msg_id != self::$BT_MSG_ID) {
            return false;
        }

        $dict_start = 2;
        $bencode_str = substr($packet, $dict_start);

        $dict_end = self::find_bencode_end($bencode_str);
        if ($dict_end === false) {
            return false;
        }

        $dict_str = substr($bencode_str, 0, $dict_end);
        $dict = Base::decode($dict_str);

        if (!is_array($dict) || !isset($dict['msg_type'])) {
            return false;
        }

        $msg_type = intval($dict['msg_type']);
        $total_dict_len = $dict_start + $dict_end;
        $piece_data = substr($packet, $total_dict_len);
        $remaining = '';

        if ($msg_type == 1 && isset($dict['piece'])) {
            $remaining = '';
        } else {
            $piece_data = '';
            $remaining = substr($packet, $total_dict_len);
        }

        return [
            'msg_type' => $msg_type,
            'piece' => isset($dict['piece']) ? intval($dict['piece']) : -1,
            'data' => $piece_data,
            'remaining' => $remaining
        ];
    }

    /**
     * 找到bencode编码数据的完整边界（栈迭代，无递归）
     */
    private static function find_bencode_end($data)
    {
        if (strlen($data) == 0 || $data[0] !== 'd') {
            return false;
        }

        $len = strlen($data);
        $stack = [];
        $pos = 0;

        $stack[] = ['type' => 'd', 'start' => $pos];
        $pos++;

        while ($pos < $len && !empty($stack)) {
            if ($data[$pos] === 'e') {
                array_pop($stack);
                $pos++;
                continue;
            }

            if ($data[$pos] >= '0' && $data[$pos] <= '9') {
                $colon_pos = strpos($data, ':', $pos);
                if ($colon_pos === false) {
                    return false;
                }
                $str_len = intval(substr($data, $pos, $colon_pos - $pos));
                $pos = $colon_pos + 1 + $str_len;
            } elseif ($data[$pos] === 'i') {
                $e_pos = strpos($data, 'e', $pos + 1);
                if ($e_pos === false) {
                    return false;
                }
                $pos = $e_pos + 1;
            } elseif ($data[$pos] === 'l' || $data[$pos] === 'd') {
                $stack[] = ['type' => $data[$pos], 'start' => $pos];
                $pos++;
            } else {
                return false;
            }
        }

        if (empty($stack)) {
            return $pos;
        }

        return false;
    }
}
