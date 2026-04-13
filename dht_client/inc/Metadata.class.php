<?php

/**
 * Metadata 下载类
 * 基于 BEP 0009 (Extension for Peers to Send Metadata Files) 实现
 */
class Metadata
{
    public static $_bt_protocol = 'BitTorrent protocol';
    public static $BT_MSG_ID = 20;
    public static $EXT_HANDSHAKE_ID = 0;
    public static $PIECE_LENGTH = 16384;

    /**
     * 下载种子元数据
     * @param Swoole\Client $client 已连接的TCP客户端
     * @param string $infohash 20字节的infohash
     * @return array|false 成功返回元数据数组，失败返回false
     */
    public static function download_metadata($client, $infohash)
    {
        $result = false;
        $_data = [];

        try {
            $start_time = microtime(true);
            $total_timeout = 30;

            // 1. BT握手
            $packet = self::send_handshake($client, $infohash);
            if ($packet === false || microtime(true) - $start_time > $total_timeout) {
                return false;
            }

            if (!self::check_handshake($packet, $infohash)) {
                return false;
            }

            // 2. 扩展握手
            $packet = self::send_ext_handshake($client);
            if ($packet === false || microtime(true) - $start_time > $total_timeout) {
                return false;
            }

            // 3. 解析扩展握手响应，获取ut_metadata id和metadata_size
            $ext_info = self::parse_ext_handshake($packet);
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

            // 4. 逐piece请求元数据
            $received_pieces = [];
            $next_piece = 0;

            while (count($received_pieces) < $piecesNum) {
                if (microtime(true) - $start_time > $total_timeout) {
                    return false;
                }

                // 请求下一个未请求的piece
                if ($next_piece < $piecesNum) {
                    if (!self::request_metadata($client, $ut_metadata, $next_piece)) {
                        return false;
                    }
                    $next_piece++;
                }

                // 接收数据
                $packet = self::recvall($client);
                if ($packet === false) {
                    return false;
                }

                // 解析一个或多个piece响应
                while (strlen($packet) > 0) {
                    $piece_result = self::parse_metadata_piece($packet);
                    if ($piece_result === false) {
                        break;
                    }

                    if ($piece_result['msg_type'] == 1 && isset($piece_result['piece'])) {
                        $piece_index = $piece_result['piece'];
                        if (!isset($received_pieces[$piece_index]) && $piece_index < $piecesNum) {
                            $received_pieces[$piece_index] = $piece_result['data'];
                        }
                    } elseif ($piece_result['msg_type'] == 2) {
                        // reject消息，对端拒绝提供元数据
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
     * 发送元数据piece请求 (BEP 0009)
     * @param Swoole\Client $client
     * @param int $ut_metadata 扩展消息ID
     * @param int $piece piece索引
     * @return bool 是否发送成功
     */
    public static function request_metadata($client, $ut_metadata, $piece)
    {
        $msg = chr(self::$BT_MSG_ID) . chr($ut_metadata) . Base::encode(array("msg_type" => 0, "piece" => $piece));
        $msg_len = pack("N", strlen($msg));
        $_msg = $msg_len . $msg;

        $rs = $client->send($_msg);
        return $rs !== false;
    }

    /**
     * 接收完整的数据包
     * @param Swoole\Client $client
     * @return string|false
     */
    public static function recvall($client)
    {
        // 读取4字节长度前缀
        $data_length = $client->recv(4, true);
        if ($data_length === false || strlen($data_length) != 4) {
            return false;
        }

        $length = unpack('N', $data_length)[1];
        if ($length == 0) {
            // keep-alive消息，继续读下一个
            return self::recvall($client);
        }

        // 安全限制：单个消息不超过16MB
        if ($length > 16777216) {
            return false;
        }

        // 读取指定长度的数据
        $data = '';
        $remaining = $length;
        while ($remaining > 0) {
            $chunk_size = min($remaining, 65536);
            $chunk = $client->recv($chunk_size, true);
            if ($chunk === false || $chunk === '') {
                return false;
            }
            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        return $data;
    }

    /**
     * 发送BT协议握手
     * @param Swoole\Client $client
     * @param string $infohash
     * @return string|false 返回握手响应或false
     */
    public static function send_handshake($client, $infohash)
    {
        $bt_protocol = self::$_bt_protocol;
        $bt_header = chr(strlen($bt_protocol)) . $bt_protocol;
        // 保留字节：第5字节bit 0 = 扩展协议支持 (0x10)
        $ext_bytes = "\x00\x00\x00\x00\x00\x10\x00\x00";
        $peer_id = Base::get_node_id();
        $packet = $bt_header . $ext_bytes . $infohash . $peer_id;

        $rs = $client->send($packet);
        if ($rs === false) {
            return false;
        }

        // BT握手响应固定68字节：1 + 19 + 8 + 20 + 20
        $data = $client->recv(68, true);
        if ($data === false || strlen($data) < 68) {
            return false;
        }

        return $data;
    }

    /**
     * 验证BT握手响应
     * @param string $packet 握手响应数据
     * @param string $self_infohash 期望的infohash
     * @return bool
     */
    public static function check_handshake($packet, $self_infohash)
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

        // 检查保留字节中是否支持扩展协议（第5字节bit 5 = 0x10）
        $reserved = substr($packet, 1 + $bt_header_len, 8);
        if ((ord($reserved[5]) & 0x10) === 0) {
            return false;
        }

        // 验证infohash匹配
        $infohash = substr($packet, 1 + $bt_header_len + 8, 20);
        if ($infohash !== $self_infohash) {
            return false;
        }

        return true;
    }

    /**
     * 发送扩展握手
     * @param Swoole\Client $client
     * @return string|false
     */
    public static function send_ext_handshake($client)
    {
        $msg = chr(self::$BT_MSG_ID) . chr(self::$EXT_HANDSHAKE_ID) . Base::encode(array("m" => array("ut_metadata" => 1)));
        $msg_len = pack("N", strlen($msg));
        $msg = $msg_len . $msg;

        $rs = $client->send($msg);
        if ($rs === false) {
            return false;
        }

        $data = self::recvall($client);
        if ($data === false || $data === '') {
            return false;
        }

        return $data;
    }

    /**
     * 解析扩展握手响应，获取ut_metadata和metadata_size
     * @param string $data 扩展握手响应
     * @return array|false ['ut_metadata' => int, 'metadata_size' => int] 或 false
     */
    public static function parse_ext_handshake($data)
    {
        // 扩展握手消息格式：[1字节BT_MSG_ID=0] + [1字节ext_id=0] + [bencode字典]
        // 但recvall已去掉了长度前缀，所以数据从BT_MSG_ID开始
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
     * @param string $packet 数据包（不含长度前缀）
     * @return array|false ['msg_type' => int, 'piece' => int, 'data' => string, 'remaining' => string] 或 false
     */
    public static function parse_metadata_piece(&$packet)
    {
        if (strlen($packet) < 2) {
            return false;
        }

        $msg_id = ord($packet[0]);
        if ($msg_id != self::$BT_MSG_ID) {
            return false;
        }

        // 尝试找到bencode字典的结尾 "e" 标记
        $dict_start = 2;
        $bencode_str = substr($packet, $dict_start);

        // 找到bencode字典的完整边界
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
            // data消息：piece数据紧跟在bencode字典之后
            $remaining = '';
        } else {
            // reject或其他消息，没有额外数据
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
     * 找到bencode编码数据的完整边界
     * @param string $data 从字典起始位置开始的数据
     * @return int|false bencode数据的字节长度，或false
     */
    private static function find_bencode_end($data)
    {
        if (strlen($data) == 0 || $data[0] !== 'd') {
            return false;
        }

        $pos = 1; // 跳过起始的 'd'
        $len = strlen($data);

        while ($pos < $len) {
            if ($data[$pos] === 'e') {
                return $pos + 1; // 包含结尾的 'e'
            }

            if ($data[$pos] >= '0' && $data[$pos] <= '9') {
                // 字符串：格式 <length>:<data>
                $colon_pos = strpos($data, ':', $pos);
                if ($colon_pos === false) {
                    return false;
                }
                $str_len = intval(substr($data, $pos, $colon_pos - $pos));
                $pos = $colon_pos + 1 + $str_len;
            } elseif ($data[$pos] === 'i') {
                // 整数：格式 i<number>e
                $e_pos = strpos($data, 'e', $pos + 1);
                if ($e_pos === false) {
                    return false;
                }
                $pos = $e_pos + 1;
            } elseif ($data[$pos] === 'l' || $data[$pos] === 'd') {
                // 嵌套列表或字典，递归查找
                $inner_end = self::find_bencode_end(substr($data, $pos));
                if ($inner_end === false) {
                    return false;
                }
                $pos += $inner_end;
            } else {
                return false;
            }
        }

        return false;
    }
}
