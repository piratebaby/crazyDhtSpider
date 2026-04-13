<?php

class DhtServer
{
    /**
     * 解析域名为IP地址，同时支持 IPv4 和 IPv6
     * @param string $hostname 域名
     * @return array 返回解析到的IP地址列表，每个元素为 ['ip' => string, 'family' => 'ipv4'|'ipv6']
     */
    public static function resolve_hostname($hostname)
    {
        $results = [];

        // 使用 dns_get_record 同时查询 A 和 AAAA 记录
        if (function_exists('dns_get_record')) {
            // 查询 A 记录（IPv4）
            $a_records = @dns_get_record($hostname, DNS_A);
            if (is_array($a_records)) {
                foreach ($a_records as $record) {
                    if (isset($record['ip']) && filter_var($record['ip'], FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
                        $results[] = ['ip' => $record['ip'], 'family' => 'ipv4'];
                    }
                }
            }

            // 查询 AAAA 记录（IPv6）
            $aaaa_records = @dns_get_record($hostname, DNS_AAAA);
            if (is_array($aaaa_records)) {
                foreach ($aaaa_records as $record) {
                    if (isset($record['ipv6']) && filter_var($record['ipv6'], FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
                        $results[] = ['ip' => $record['ipv6'], 'family' => 'ipv6'];
                    }
                }
            }
        }

        // 兜底：如果 dns_get_record 没有结果，使用 gethostbyname
        if (empty($results)) {
            $ipv4 = gethostbyname($hostname);
            if ($ipv4 !== $hostname && filter_var($ipv4, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
                $results[] = ['ip' => $ipv4, 'family' => 'ipv4'];
            }
        }

        return $results;
    }

    /**
     * 加入dht网络
     * @param array $table 路由表
     * @param array $bootstrap_nodes 引导节点列表
     */
    public static function join_dht($table, $bootstrap_nodes)
    {
        if (empty($bootstrap_nodes)) {
            return;
        }
        
        $is_empty = ($table instanceof Swoole\Table) ? ($table->count() == 0) : (count($table) == 0);

        if ($is_empty) {
            foreach ($bootstrap_nodes as $node) {
                $resolved = self::resolve_hostname($node[0]);
                foreach ($resolved as $addr) {
                    self::find_node(array($addr['ip'], $node[1]));
                }
            }
        }
    }

    public static function auto_find_node($table, $bootstrap_nodes)
    {
        // 使用协程批量发送find_node请求，提高并发性能
        $nodes = [];
        
        if ($table instanceof Swoole\Table) {
            // 处理Swoole\Table格式
            foreach ($table as $key => $node) {
                $nodes[] = [$node['ip'], $node['port'], $node['nid']];
            }
        } else {
            // 处理普通数组格式
            foreach ($table as $node) {
                $nodes[] = [$node->ip, $node->port, $node->nid];
            }
        }
        
        // 使用Swoole的协程并发发送请求
        if (!empty($nodes)) {
            // 降低并发数，避免过多连接导致系统资源耗尽
            $concurrency = 20; // 从50降低到20，减少每个批次的任务量
            $node_chunks = array_chunk($nodes, $concurrency);
            
            foreach ($node_chunks as $chunk) {
                // 为每个节点创建一个协程发送请求
                foreach ($chunk as $node_info) {
                    go(function () use ($node_info) {
                        list($ip, $port, $nid) = $node_info;
                        self::find_node(array($ip, $port), $nid);
                    });
                }
                // 增加批次间隔，避免瞬间发送过多请求
                // 直接使用usleep，但在协程环境中会自动被Swoole hook
                usleep(5); // 5毫秒
            }
        }
    }

    public static function find_node($address, $id = null)
    {
        global $nids;
        
        // 按目标网段选择Node ID，在同一网段使用相同的ID
        $current_nid = self::select_node_id_by_address($address);
        
        if (is_null($id)) {
            $mid = Base::get_node_id();
        } else {
            $mid = Base::get_neighbor($id, $current_nid); // 否则伪造一个相邻id
        }
        
        // 定义发送数据 认识新朋友的。
        $msg = array(
            't' => Base::entropy(2),
            'y' => 'q',
            'q' => 'find_node',
            'a' => array(
                'id' => $current_nid,
                'target' => $mid
            )
        );
        
        // 发送请求数据到对端
        self::send_response($msg, $address);
    }

    /**
     * 获取目标地址的地址族类型
     * @param string $ip IP地址
     * @return string 'ipv4' 或 'ipv6'
     */
    public static function get_address_family($ip)
    {
        return Base::is_ipv6($ip) ? 'ipv6' : 'ipv4';
    }
    
    /**
     * 按目标地址选择合适的Node ID
     * 同一网段使用相同的Node ID，提高在该区域的信誉值
     * @param array $address 目标地址 [ip, port]
     * @return string 选择的Node ID
     */
    public static function select_node_id_by_address($address)
    {
        global $nids;
        $ip = $address[0];
        
        // 检查是否为IPv6地址
        if (strpos($ip, ':') !== false) {
            // IPv6地址：使用前4个部分作为网络标识
            $ipv6_parts = explode(':', $ip);
            if (count($ipv6_parts) >= 4) {
                $network_key = implode(':', array_slice($ipv6_parts, 0, 4));
            } else {
                $network_key = $ip;
            }
        } else {
            // IPv4地址：使用前三位作为网络标识
            $ip_parts = explode('.', $ip);
            if (count($ip_parts) >= 3) {
                $network_key = $ip_parts[0] . '.' . $ip_parts[1] . '.' . $ip_parts[2];
            } else {
                $network_key = $ip;
            }
        }
        
        // 使用网段的哈希值选择Node ID，确保同一网段使用相同ID
        $hash = crc32($network_key);
        $index = abs($hash) % count($nids);
        
        return $nids[$index];
    }

    public static function send_response($msg, $address)
    {
        global $serv, $ipv6_server_fd;

        $ip = $address[0];
        if (!filter_var($ip, FILTER_VALIDATE_IP)) {
            return false;
        }
        $data = Base::encode($msg);

        // 根据目标IP类型选择正确的server fd发送
        if (Base::is_ipv6($ip)) {
            // IPv6: 使用IPv6端口的server fd发送
            if (isset($ipv6_server_fd) && $ipv6_server_fd > 0) {
                $serv->sendto($ip, $address[1], $data, $ipv6_server_fd);
            } else {
                // 如果IPv6端口未初始化，尝试直接发送（某些Swoole版本支持自动选择）
                $serv->sendto($ip, $address[1], $data);
            }
        } else {
            // IPv4: 使用默认端口发送
            $serv->sendto($ip, $address[1], $data);
        }
    }
    
    /**
     * 发送get_peers请求
     * @param array $address 目标地址 [ip, port]
     * @param string $infohash 要查询的infohash
     * @param string $nid 目标节点ID
     * @return void
     */
    public static function get_peers($address, $infohash, $nid = null)
    {
        global $nids;
        
        // 按目标网段选择Node ID，在同一网段使用相同的ID
        $current_nid = self::select_node_id_by_address($address);
        
        // 如果提供了节点ID，伪造一个相邻ID，否则使用当前ID
        $mid = is_null($nid) ? $current_nid : Base::get_neighbor($nid, $current_nid);
        
        // 定义get_peers请求消息
        $msg = array(
            't' => Base::entropy(2),
            'y' => 'q',
            'q' => 'get_peers',
            'a' => array(
                'id' => $current_nid,
                'info_hash' => $infohash
            )
        );
        
        // 发送请求数据到对端
        self::send_response($msg, $address);
    }
}