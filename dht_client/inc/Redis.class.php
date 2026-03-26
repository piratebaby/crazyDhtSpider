<?php

class RedisPool
{
    private static $instances = [];
    private $config = [];
    private $pool;
    private $max_connections = 10;
    private $current_count = 0; // 替换 Atomic，Worker 内协程安全
    private $ping_interval = 30;

    private function __construct() {}

    public static function getInstance()
    {
        $pid = getmypid();
        if (!isset(self::$instances[$pid])) {
            self::$instances[$pid] = new self();
        }
        return self::$instances[$pid];
    }

    public function init($config = [])
    {
        $this->config = $config;
        if (!empty($config['max_connections'])) {
            $this->max_connections = (int)$config['max_connections'];
        }

        // 使用 Channel 作为协程池容器
        $this->pool = new Swoole\Coroutine\Channel($this->max_connections);
        return $this;
    }

    private function createConnection()
    {
        try {
            $redis = new \Redis();
            $timeout = $this->config['timeout'] ?? 3;

            // 1. 建立连接并检查布尔返回值
            $connected = $redis->connect(
                $this->config['host'],
                $this->config['port'],
                $timeout
            );

            if (!$connected) {
                return null;
            }

            if (!empty($this->config['password'])) {
                $redis->auth($this->config['password']);
            }

            if (!empty($this->config['database'])) {
                $redis->select($this->config['database']);
            }

            // 2. 【关键】开启异常模式，确保网络错误能被 execute() 的 catch 捕获
            $redis->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
            $redis->setOption(\Redis::OPT_READ_TIMEOUT, 3);
            if (defined('\Redis::OPT_EXCEPTION')) {
                $redis->setOption(\Redis::OPT_EXCEPTION, true);
            }

            return [
                'redis' => $redis,
                'time' => time()
            ];
        } catch (\Throwable $e) {
            return null;
        }
    }

    public function getConnection()
    {
        // 1. 尝试从池中获取
        $conn = $this->pool->pop(0.5); // 稍微加长等待时间，减少忙等
        if ($conn) {
            // 检查连接是否可用
            if (time() - $conn['time'] < $this->ping_interval) {
                return $conn['redis'];
            }
            
            // 超过心跳时间，测试一次 PING
            try {
                if ($conn['redis']->ping()) {
                    return $conn['redis'];
                }
            } catch (\Throwable $e) {
                // PING 失败，销毁连接
            }
            
            $this->destroyConnection($conn['redis']);
        }

        // 2. 尝试创建新连接（严格控制计数器）
        if ($this->current_count < $this->max_connections) {
            $this->current_count++; // 先占位
            $newConn = $this->createConnection();
            if ($newConn) {
                return $newConn['redis'];
            }
            $this->current_count--; // 创建失败，归还位子
        }

        // 3. 最后尝试再次从池中阻塞获取（兜底）
        $conn = $this->pool->pop(1.0);
        return $conn ? $conn['redis'] : null;
    }

    public function returnConnection($redis)
    {
        if (!$redis instanceof \Redis) {
            return;
        }

        // 如果池子没满，放回去；满了则销毁
        $data = ['redis' => $redis, 'time' => time()];
        if (!$this->pool->push($data, 0.001)) {
            $this->destroyConnection($redis);
        }
    }

    private function destroyConnection($redis)
    {
        try {
            $redis->close();
        } catch (\Throwable $e) {
        } finally {
            $this->current_count--;
            if ($this->current_count < 0) $this->current_count = 0;
        }
    }

    public function execute(callable $callback)
    {
        $redis = $this->getConnection();
        if (!$redis) {
            return false;
        }

        try {
            $result = $callback($redis);
            $this->returnConnection($redis); // 正常逻辑，还回池子
            return $result;
        } catch (\Throwable $e) {
            // 只要报错（网络超时、断开等），一律销毁，不传毒
            $this->destroyConnection($redis);
            return false;
        }
    }

    // --- 业务方法 ---

    public function exists($infohash)
    {
        return $this->execute(function($redis) use ($infohash) {
            $infohash_bin = $this->normalizeInfohash($infohash);
            $set_key = ($this->config['prefix'] ?? '') . 'infohashes';
            return (bool)$redis->sIsMember($set_key, $infohash_bin);
        });
    }

    private function normalizeInfohash($infohash)
    {
        if (strlen($infohash) === 40 && ctype_xdigit($infohash)) {
            return hex2bin($infohash);
        }
        return (strlen($infohash) === 20) ? $infohash : hex2bin($infohash);
    }
}