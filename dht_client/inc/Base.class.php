<?php

require_once 'Bencode.class.php';

/**
 * 基础操作类
 */
class Base
{
    /**
     * 把字符串转换为数字
     * @param string $str 要转换的字符串
     * @return   string 转换后的字符串
     */
    static public function hash2int($str)
    {
        return hexdec(bin2hex($str));
    }

    /**
     * 生成随机字符串
     * @param integer $length 要生成的长度
     * @return   string 生成的字符串
     */
    static public function entropy($length = 20)
    {
        $str = '';

        for ($i = 0; $i < $length; $i++)
            $str .= chr(mt_rand(0, 255));

        return $str;
    }

    /**
     * CRC32C (Castagnoli) 实现
     * @param string $data 要计算CRC32C的数据
     * @return int CRC32C值
     */
    static private function crc32c_calculate($data)
    {
        // 检查是否支持crc32c算法
        if (in_array('crc32c', hash_algos())) {
            return hexdec(hash('crc32c', $data));
        }
        // 如果没有crc32c支持，使用标准crc32作为备选
        return crc32($data);
    }
    
    /**
     * 根据 BEP 42 规范生成 Node ID
     * BEP 42: IP 与 Node ID 的绑定协议
     * 支持 IPv4 和 IPv6 地址
     * @param string|null $ip 公网 IP 地址（IPv4 或 IPv6）
     * @return string 20字节的二进制 Node ID
     */
    static public function get_node_id($ip = null)
    {
        // 如果没有提供IP，从配置文件获取
        if (is_null($ip)) {
            // 从配置文件获取IP（只在第一次调用时加载）
            static $config_ip = null;
            if (is_null($config_ip)) {
                $config = require __DIR__ . '/../config.php';
                // 只使用local_node_ip配置项
                $config_ip = $config['application']['local_node_ip'];
            }
            $ip = $config_ip;
        }

        // 判断是 IPv4 还是 IPv6
        if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
            return self::get_node_id_ipv6($ip);
        } elseif (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
            return self::get_node_id_ipv4($ip);
        }

        // 如果IP格式不正确，生成随机node_id
        return self::entropy(20);
    }

    /**
     * 根据 BEP 42 规范为 IPv4 地址生成 Node ID
     * @param string $ip IPv4 地址
     * @return string 20字节的二进制 Node ID
     */
    static private function get_node_id_ipv4($ip)
    {
        $ip_parts = array_map('intval', explode('.', $ip));

        // 1. 准备 IP 的前三个字节，并按规范进行掩码处理
        // Mask: 0x03, 0x0f, 0x3f, 0x00
        $r1 = $ip_parts[0] & 0x03;
        $r2 = $ip_parts[1] & 0x0F;
        $r3 = $ip_parts[2] & 0x3F;

        // 2. 生成随机种子 (Seed)，BEP 42 要求取随机数的最后一个字节
        $seed = random_int(0, 255);
        $r = $seed & 0x07; // 取种子后3位

        // 3. 组合成 24-bit 的输入值用于 CRC32C 计算
        // 逻辑：(r << 21) | (ip[0] << 16) | (ip[1] << 8) | ip[2]
        $v = ($r << 21) | ($r1 << 16) | ($r2 << 8) | $r3;

        // 4. 计算 CRC32C (Castagnoli)
        $hash = self::crc32c_calculate(pack('N', $v));

        // 5. 构造 Node ID
        // 前 21 bits (约 2.6 字节) 必须匹配 hash
        $node_id = "";
        $node_id .= chr(($hash >> 24) & 0xFF);
        $node_id .= chr(($hash >> 16) & 0xFF);
        $node_id .= chr((($hash >> 8) & 0xF8) | (random_int(0, 255) & 0x07)); // 第3字节前5位是hash，后3位随机
        
        // 剩下的 17 字节填充随机数
        for ($i = 0; $i < 17; $i++) {
            $node_id .= chr(random_int(0, 255));
        }
        
        // 6. 最后一位强制设为种子，以便他人验证
        $node_id[19] = chr($seed);

        return $node_id;
    }

    /**
     * 根据 BEP 42 规范为 IPv6 地址生成 Node ID
     * BEP 42 对 IPv6 的扩展：使用 IPv6 地址的前 8 字节进行 CRC32C 计算
     * 掩码: 每个字节依次为 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff
     * @param string $ip IPv6 地址
     * @return string 20字节的二进制 Node ID
     */
    static private function get_node_id_ipv6($ip)
    {
        // 将 IPv6 地址转换为 16 字节二进制
        $ip_bin = @inet_pton($ip);
        if ($ip_bin === false || strlen($ip_bin) !== 16) {
            return self::entropy(20);
        }

        // BEP 42 IPv6 掩码：对前 8 字节进行掩码处理
        $masks = [0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff];
        $masked = '';
        for ($i = 0; $i < 8; $i++) {
            $masked .= chr(ord($ip_bin[$i]) & $masks[$i]);
        }

        // 生成随机种子
        $seed = random_int(0, 255);
        $r = $seed & 0x07;

        // 将 r 的 3 位插入到第一个字节的高 3 位
        $masked[0] = chr((ord($masked[0]) & 0x1F) | ($r << 5));

        // 计算 CRC32C
        $hash = self::crc32c_calculate($masked);

        // 构造 Node ID：前 21 bits 匹配 hash
        $node_id = '';
        $node_id .= chr(($hash >> 24) & 0xFF);
        $node_id .= chr(($hash >> 16) & 0xFF);
        $node_id .= chr((($hash >> 8) & 0xF8) | (random_int(0, 255) & 0x07));

        // 剩下的 17 字节填充随机数
        for ($i = 0; $i < 17; $i++) {
            $node_id .= chr(random_int(0, 255));
        }

        // 最后一位设为种子
        $node_id[19] = chr($seed);

        return $node_id;
    }

    /**
     * 判断IP地址是否为IPv6
     * @param string $ip IP地址
     * @return bool
     */
    static public function is_ipv6($ip)
    {
        return filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) !== false;
    }

    static public function get_neighbor($target, $nid)
    {
        // 优化：使用前15位与目标节点一致，提高邻居相似度
        return substr($target, 0, 15) . substr($nid, 15, 5);
    }

    /**
     * bencode编码
     * @param mixed $msg 要编码的数据
     * @return   string 编码后的数据
     */
    static public function encode($msg)
    {
        return Bencode::encode($msg);
    }

    /**
     * bencode解码
     * @param string $msg 要解码的数据
     * @return   mixed      解码后的数据
     */
    static public function decode($msg)
    {
        return Bencode::decode($msg);
    }

    /**
     * 对nodes列表编码
     * 根据地址族类型编码，DHT协议要求同一个nodes字段中只能包含同一地址族的节点
     * @param mixed $nodes 要编码的列表（Node对象数组或关联数组数组）
     * @param string $address_family 地址族：'ipv4' 或 'ipv6'，默认 'ipv4'
     * @return string 编码后的数据
     */
    static public function encode_nodes($nodes, $address_family = 'ipv4')
    {
        // 判断当前nodes列表是否为空
        if (count($nodes) == 0)
            return $nodes;

        $n = '';
        $is_ipv6 = ($address_family === 'ipv6');

        // 循环对node进行编码
        foreach ($nodes as $node) {
            // 兼容Node对象和关联数组两种格式
            $node_ip = is_array($node) ? ($node['ip'] ?? '') : $node->ip;
            $node_nid = is_array($node) ? ($node['nid'] ?? '') : $node->nid;
            $node_port = is_array($node) ? ($node['port'] ?? 0) : $node->port;

            $node_is_ipv6 = self::is_ipv6($node_ip);

            // 只编码与目标地址族匹配的节点
            if ($is_ipv6 && $node_is_ipv6) {
                // IPv6地址编码：20字节nid + 16字节IPv6 + 2字节端口
                $ipv6_packed = @inet_pton($node_ip);
                if ($ipv6_packed) {
                    $n .= pack('a20a16n', $node_nid, $ipv6_packed, $node_port);
                }
            } elseif (!$is_ipv6 && !$node_is_ipv6) {
                // IPv4地址编码：20字节nid + 4字节IPv4 + 2字节端口
                $n .= pack('a20Nn', $node_nid, ip2long($node_ip), $node_port);
            }
        }

        return $n;
    }

    /**
     * 对nodes列表解码
     * 使用源IP地址判断节点格式，避免依赖不可靠的长度猜测
     * @param string $msg 要解码的数据
     * @param string $source_ip 来源IP地址，用于判断节点格式（IPv4/IPv6）
     * @return mixed 解码后的数据
     */
    static public function decode_nodes($msg, $source_ip = null)
    {
        $n = array();
        $msg_len = strlen($msg);

        if ($msg_len == 0) {
            return $n;
        }

        // 优先使用源IP判断地址族，这是最可靠的方式
        if ($source_ip !== null && self::is_ipv6($source_ip)) {
            // 来源是IPv6，按IPv6格式解析（38字节/节点）
            if ($msg_len % 38 == 0) {
                foreach (str_split($msg, 38) as $s) {
                    $r = unpack('a20nid/a16ip/np', $s);
                    $ip = @inet_ntop($r['ip']);
                    if ($ip !== false) {
                        $n[] = new Node($r['nid'], $ip, $r['p']);
                    }
                }
                return $n;
            }
        }

        // 来源是IPv4或未知，优先按IPv4格式解析（26字节/节点）
        if ($msg_len % 26 == 0) {
            foreach (str_split($msg, 26) as $s) {
                $r = unpack('a20nid/Nip/np', $s);
                $ip = long2ip($r['ip']);
                if ($ip !== false) {
                    $n[] = new Node($r['nid'], $ip, $r['p']);
                }
            }
        } elseif ($msg_len % 38 == 0) {
            // 无法按IPv4解析，尝试IPv6格式
            foreach (str_split($msg, 38) as $s) {
                $r = unpack('a20nid/a16ip/np', $s);
                $ip = @inet_ntop($r['ip']);
                if ($ip !== false) {
                    $n[] = new Node($r['nid'], $ip, $r['p']);
                }
            }
        }

        return $n;
    }
}