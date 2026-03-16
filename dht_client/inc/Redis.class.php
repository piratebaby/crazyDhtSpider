<?php

class RedisPool
{
    private static $instances = [];
    private $config = [];
    private $pool;
    private $max_connections = 10;
    private $current_connections;
    private $ping_interval = 30;

    private function __construct()
    {
    }

    public static function getInstance()
    {
        // 使用进程ID作为键，确保每个Worker进程都有独立的实例
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
            $this->max_connections = min($config['max_connections'], 50);
        }

        // 初始化原子计数器
        $this->current_connections = new Swoole\Atomic(0);

        // 连接池容量设置为最大连接数
        $this->pool = new Swoole\Coroutine\Channel($this->max_connections);

        return $this;
    }

    private function createConnection()
    {
        try {

            $redis = new Redis();

            $timeout = $this->config['timeout'] ?? 3;

            $redis->connect(
                $this->config['host'],
                $this->config['port'],
                $timeout
            );

            if (!empty($this->config['password'])) {
                $redis->auth($this->config['password']);
            }

            if (!empty($this->config['database'])) {
                $redis->select($this->config['database']);
            }

            // 设置连接选项
            if (defined('Redis::OPT_READ_TIMEOUT')) {
                $redis->setOption(Redis::OPT_READ_TIMEOUT, 3);
            }
            if (defined('Redis::OPT_CONNECT_TIMEOUT')) {
                $redis->setOption(Redis::OPT_CONNECT_TIMEOUT, $timeout);
            }

            return [
                'redis' => $redis,
                'time' => time()
            ];

        } catch (Exception $e) {
            return null;
        }
    }

    private function isConnectionValid($conn)
    {
        $redis = $conn['redis'];
        $last_used = $conn['time'];

        if (time() - $last_used < $this->ping_interval) {
            return true;
        }

        try {

            $result = $redis->ping();

            return $result === '+PONG' || $result === true;

        } catch (Exception $e) {
            return false;
        }
    }

    public function getConnection()
    {

        if (empty($this->config['host'])) {
            return null;
        }

        // 尝试从池中获取连接
        $attempts = 0;
        $max_attempts = 5;
        
        while ($attempts < $max_attempts) {
            $attempts++;
            
            // 直接从池中获取连接，处理空队列情况
            $conn = $this->pool->pop(0.1); // 100ms超时
            if ($conn) {
                if ($this->isConnectionValid($conn)) {
                    return $conn['redis'];
                }

                try {
                    $conn['redis']->close();
                } catch (Exception $e) {}

                $this->current_connections->sub(1);
            }

            // 检查是否可以创建新连接，使用原子操作避免并发问题
            if ($this->current_connections->add(1) <= $this->max_connections) {
                $conn = $this->createConnection();
                if ($conn) {
                    return $conn['redis'];
                }
                // 创建连接失败，减少计数
                $this->current_connections->sub(1);
            }

            // 短暂挂起，避免忙等
            Swoole\Coroutine::sleep(0.01);
        }

        return null;
    }

    public function returnConnection($redis)
    {
        if (!$redis instanceof Redis) {
            return;
        }

        $conn = [
            'redis' => $redis,
            'time' => time()
        ];

        // 使用非阻塞的push操作，如果失败则关闭连接
        if (!$this->pool->push($conn, 0.1)) {

            try {
                $redis->close();
            } catch (Exception $e) {}

            $this->current_connections->sub(1);
        }
    }

    public function execute(callable $callback)
    {

        $retry = 0;
        $max_retries = 3;

        while ($retry < $max_retries) {

            $redis = null;

            try {

                $redis = $this->getConnection();

                if (!$redis) {
                    $retry++;
                    Swoole\Coroutine::sleep(0.01);
                    continue;
                }

                $result = $callback($redis);

                return $result;

            } catch (Throwable $e) {

                if ($redis) {
                    try {
                        $redis->close();
                    } catch (Exception $e) {}

                    $this->current_connections->sub(1);
                    $redis = null;
                }

                $retry++;
                Swoole\Coroutine::sleep(0.01);
                continue;

            } finally {

                if ($redis) {
                    $this->returnConnection($redis);
                }

            }

        }

        return false;
    }

    public function exists($infohash)
    {

        $result = $this->execute(function($redis) use ($infohash) {

            $infohash_bin = $this->normalizeInfohash($infohash);

            $set_key = $this->config['prefix'] . 'infohashes';

            return (bool)$redis->sIsMember($set_key, $infohash_bin);

        });

        return (bool)$result;
    }

    public function set($infohash, $expire = 86400)
    {

        return (bool)$this->execute(function($redis) use ($infohash, $expire) {

            $infohash_bin = $this->normalizeInfohash($infohash);

            $set_key = $this->config['prefix'] . 'infohashes';

            $result = $redis->sAdd($set_key, $infohash_bin);

            if ($result) {
                $redis->expire($set_key, $expire);
            }

            return $result;

        });
    }

    private function normalizeInfohash($infohash)
    {

        if (strlen($infohash) === 40 && ctype_xdigit($infohash)) {
            return hex2bin($infohash);
        }

        if (strlen($infohash) === 20) {
            return $infohash;
        }

        return hex2bin(strtoupper(bin2hex($infohash)));
    }

    public function closeAll()
    {

        while (!$this->pool->isEmpty()) {

            $conn = $this->pool->pop(0.1);
            if ($conn) {
                try {
                    $conn['redis']->close();
                } catch (Exception $e) {}
            }
        }

        $this->current_connections->set(0);
    }

    /**
     * 析构函数，确保连接被正确清理
     */
    public function __destruct()
    {
        try {
            $this->closeAll();
        } catch (Throwable $e) {
            // 捕获异常，避免影响进程退出
        }
    }
}
