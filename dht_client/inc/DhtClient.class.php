<?php

class DhtClient
{

    public static $_bt_protocol = 'BitTorrent protocol';
    public static $BT_MSG_ID = 20;
    public static $EXT_HANDSHAKE_ID = 0;
    public static $PIECE_LENGTH = 16384;
    public static $announce_dedup_table = null;
    public static $stats_cache = ['data' => null, 'ts' => 0];

    /**
     * 处理接收到的find_node回复
     * @param array $msg 接收到的数据
     * @param array $address 对端链接信息
     * @return void
     */
    public static function response_action($msg, $address)
    {
        global $table;
        // 先检查接收到的信息是否正确
        if (!isset($msg['r']) || !is_array($msg['r'])) {
            return;
        }

        // 对端节点回复了我们，将其更新到路由表（更新last_seen）
        if (isset($msg['r']['id'])) {
            self::append(new Node($msg['r']['id'], $address[0], $address[1]));
        }
        
        // 处理回复中的nodes列表
        if (!isset($msg['r']['nodes'])) {
            return;
        }
        
        $nodes_data = $msg['r']['nodes'];
        // 检查nodes数据是否为字符串
        if (!is_string($nodes_data) || strlen($nodes_data) < 26) {
            return;
        }
        
        // 对nodes数据进行解码，传递源IP以正确判断地址族
        $nodes = Base::decode_nodes($nodes_data, $address[0]);
        
        // 检查解码结果是否有效
        if (!is_array($nodes) || empty($nodes)) {
            return;
        }
        
        // 对nodes循环处理
        foreach ($nodes as $node) {
            // 验证node对象是否有效
            if ($node instanceof Node) {
                // 将node加入到路由表中
                self::append($node);
            }
        }
    }

    /**
     * 处理对端发来的请求
     * @param array $msg 接收到的请求数据
     * @param array $address 对端链接信息
     * @return void
     */
    public static function request_action($msg, $address)
    {
        switch ($msg['q']) {
            case 'ping'://确认你是否在线
                //echo '朋友'.$address[0].'正在确认你是否在线'.PHP_EOL;
                self::on_ping($msg, $address);
                break;
            case 'find_node': //向服务器发出寻找节点的请求
                //echo '朋友'.$address[0].'向你发出寻找节点的请求'.PHP_EOL;
                //self::on_find_node($msg, $address);
                break;
            case 'get_peers':
                //echo '朋友'.$address[0].'向你发出查找资源的请求'.PHP_EOL;
                // 处理get_peers请求
                self::on_get_peers($msg, $address);
                break;
            case 'announce_peer':
                //echo '朋友' . $address[0] . '找到资源了 通知你一声' . PHP_EOL;
                // 处理announce_peer请求
                self::on_announce_peer($msg, $address);
                break;
            default:
                break;
        }
    }

    /**
     * 添加node到路由表
     * @param Node $node node模型
     * @return boolean       是否添加成功
     */
    public static function append($node)
    {
        global $nids, $table, $ip_port_index, $config;
        // 检查node id是否正确
        if (!isset($node->nid[19])) {
            return false;
        }

        // 检查是否为自身node id（10个中的任何一个）
        if (in_array($node->nid, $nids)) {
            return false;
        }

        // 检查端口有效性
        if ($node->port < 1 || $node->port > 65535) {
            return false;
        }

        if ($table instanceof Swoole\Table) {
            // 使用Swoole\Table实现
            $now = time();

            // IPv6地址使用[ip]:port格式，避免与IPv6地址中的冒号混淆
            $ip_port_key = Base::is_ipv6($node->ip) 
                ? '[' . $node->ip . ']:' . $node->port 
                : $node->ip . ':' . $node->port;
            
            // 检查是否已存在相同节点ID
            if ($table->exist($node->nid)) {
                // 节点已存在，获取旧的IP+端口信息
                $old_node = $table->get($node->nid);
                $old_ip_port_key = Base::is_ipv6($old_node['ip']) 
                    ? '[' . $old_node['ip'] . ']:' . $old_node['port'] 
                    : $old_node['ip'] . ':' . $old_node['port'];
                
                // 更新节点信息（包括last_seen时间戳）
                $table->set($node->nid, [
                    'nid' => $node->nid,
                    'ip' => $node->ip,
                    'port' => $node->port,
                    'last_seen' => $now
                ]);
                
                // 如果IP或端口发生变化，更新索引表
                if ($old_ip_port_key != $ip_port_key) {
                    $ip_port_index->del($old_ip_port_key);
                    $ip_port_index->set($ip_port_key, ['nid' => $node->nid]);
                }
                
                return $table->count();
            }

            // 使用IP+端口索引表快速检查是否已存在相同IP+端口的节点
            $existing_nid = null;
            if ($ip_port_index->exist($ip_port_key)) {
                $existing_nid = $ip_port_index->get($ip_port_key)['nid'];
            }

            if ($existing_nid !== null && $existing_nid != $node->nid) {
                // 删除旧节点
                $table->del($existing_nid);
                $ip_port_index->del($ip_port_key);
            }

            // 添加新节点
            $table->set($node->nid, [
                'nid' => $node->nid,
                'ip' => $node->ip,
                'port' => $node->port,
                'last_seen' => $now
            ]);
            
            // 更新索引表
            $ip_port_index->set($ip_port_key, ['nid' => $node->nid]);
            
            return $table->count();
        } else {
            // 使用普通数组实现（兼容旧版本）
            $existing_key = -1;
            foreach ($table as $k => $existing_node) {
                if ($existing_node->nid == $node->nid || 
                    ($existing_node->ip == $node->ip && $existing_node->port == $node->port)) {
                    $existing_key = $k;
                    break;
                }
            }

            if ($existing_key >= 0) {
                // 如果节点已存在，更新它的位置到路由表末尾（最近活跃）
                unset($table[$existing_key]);
                $table = array_values($table);
                array_push($table, $node);
                return count($table);
            }

            return array_push($table, $node);
        }
    }

    public static function on_ping($msg, $address)
    {
        global $nids;
        // 使用第一个node_id作为回复，或者可以选择更合适的策略
        $current_nid = $nids[0];
        // 获取对端node id
        $id = $msg['a']['id'];
        // 生成回复数据
        $msg = array(
            't' => $msg['t'],
            'y' => 'r',
            'r' => array(
                'id' => Base::get_neighbor($id, $current_nid)
            )
        );

        // 将node加入路由表
        self::append(new Node($id, $address[0], $address[1]));
        // 发送回复数据
        DhtServer::send_response($msg, $address);
    }

    public static function on_find_node($msg, $address)
    {

        global $nids;
        // 使用第一个node_id作为回复，或者可以选择更合适的策略
        $current_nid = $nids[0];
        // 获取对端node id
        $id = $msg['a']['id'];
        
        // 根据对端地址族获取节点列表并编码
        $address_family = DhtServer::get_address_family($address[0]);

        // 生成回复数据
        $msg = array(
            't' => $msg['t'],
            'y' => 'r',
            'r' => array(
                'id' => Base::get_neighbor($id, $current_nid),
                'nodes' => Base::encode_nodes(self::get_nodes(16, $address_family), $address_family)
            )
        );
        // 将node加入路由表
        self::append(new Node($id, $address[0], $address[1]));
        // 发送回复数据
        DhtServer::send_response($msg, $address);
    }

    /**
     * 处理get_peers请求
     * @param array $msg 接收到的get_peers请求数据
     * @param array $address 对端链接信息
     * @return void
     */
    public static function on_get_peers($msg, $address)
    {
        global $nids;
        // 使用第一个node_id作为回复，或者可以选择更合适的策略
        $current_nid = $nids[0];

        // 获取info_hash信息
        $infohash = $msg['a']['info_hash'];
        // 获取node id
        $id = $msg['a']['id'];

        // 生成回复数据
        $msg = array(
            't' => $msg['t'],
            'y' => 'r',
            'r' => array(
                'id' => Base::get_neighbor($id, $current_nid),
                'nodes' => "",
                'token' => substr($infohash, 0, 2)
            )
        );


        // 将node加入路由表
        self::append(new Node($id, $address[0], $address[1]));
        // 向对端发送回复数据
        DhtServer::send_response($msg, $address);
    }

    /**
     * 处理announce_peer请求
     * @param array $msg 接收到的announce_peer请求数据
     * @param array $address 对端链接信息
     * @return void
     */
    public static function on_announce_peer($msg, $address)
    {
        global $nids, $config, $serv, $task_num;
        // 使用第一个node_id作为回复，或者可以选择更合适的策略
        $current_nid = $nids[0];
        $infohash = $msg['a']['info_hash'];
        $port = $msg['a']['port'];
        $token = $msg['a']['token'];
        $id = $msg['a']['id'];
        $tid = $msg['t'];

        // 验证token是否正确
        if (substr($infohash, 0, 2) != $token) return;

        if (isset($msg['a']['implied_port']) && $msg['a']['implied_port'] != 0) {
            $port = $address[1];
        }

        if ($port >= 65536 || $port <= 0) {
            return;
        }

        if ($tid == '') {
            //return;
        }

        // 生成回复数据
        $msg = array(
            't' => $msg['t'],
            'y' => 'r',
            'r' => array(
                'id' => $current_nid
            )
        );

        // 使用Swoole\Table做ip:infohash短时间去重，替代简单的$last_ip过滤
        $dedup_key = $address[0] . ':' . $infohash;
        if (self::$announce_dedup_table !== null && self::$announce_dedup_table->exists($dedup_key)) {
            // 同一IP同一infohash在去重窗口内重复，跳过（但仍回复DHT响应）
            DhtServer::send_response($msg, $address);
            return;
        }
        if (self::$announce_dedup_table !== null) {
            self::$announce_dedup_table->set($dedup_key, ['ts' => time()]);
        }
        $ip = $address[0];
        // 发送请求回复
        DhtServer::send_response($msg, $address);

        // 检查当前任务数量，如果过多则暂时不提交新任务（使用缓存stats，每秒更新）
        $now = time();
        if (self::$stats_cache['data'] === null || $now - self::$stats_cache['ts'] >= 1) {
            self::$stats_cache['data'] = $serv->stats();
            self::$stats_cache['ts'] = $now;
        }
        $stats = self::$stats_cache['data'];
        $threshold = $config['application']['task_threshold'] ?? 0.9;
        $max_tasks = max(1, floor($stats['task_worker_num'] * $threshold));
        if ($stats['tasking_num'] >= $max_tasks) {
            return;
        }

        $task_id = MySwoole::enqueueDownload($serv, $ip, $port, $infohash);
        //echo "Dispath AsyncTask: [id=$task_id]\n";
        return;
    }

    /**
     * 从路由表获取节点列表
     * @param int $len 获取数量
     * @param string $address_family 地址族过滤：'ipv4'、'ipv6' 或 'all'
     * @return array 节点列表
     */
    public static function get_nodes($len = 8, $address_family = 'all')
    {
        global $table;

        if ($table instanceof Swoole\Table) {
            // 处理Swoole\Table格式
            $nodes = array();
            $count = 0;
            foreach ($table as $key => $node) {
                if ($count >= $len) {
                    break;
                }
                // 按地址族过滤
                if ($address_family !== 'all') {
                    $node_is_ipv6 = Base::is_ipv6($node['ip']);
                    if (($address_family === 'ipv6' && !$node_is_ipv6) || 
                        ($address_family === 'ipv4' && $node_is_ipv6)) {
                        continue;
                    }
                }
                $nodes[] = $node;
                $count++;
            }
            return $nodes;
        } else {
            // 处理普通数组格式
            if (count($table) <= $len)
                return $table;

            $nodes = array();
            $count = 0;
            foreach ($table as $node) {
                if ($count >= $len) {
                    break;
                }
                // 按地址族过滤
                if ($address_family !== 'all') {
                    $node_is_ipv6 = Base::is_ipv6($node->ip);
                    if (($address_family === 'ipv6' && !$node_is_ipv6) || 
                        ($address_family === 'ipv4' && $node_is_ipv6)) {
                        continue;
                    }
                }
                $nodes[] = $node;
                $count++;
            }
            return $nodes;
        }
    }



}