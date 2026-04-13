<?php
/**
 * MySQL连接池类
 */
class MysqlPool
{
    private static $instances = [];
    private $config = array();
    private $pool = array();
    private $max_connections = 10;
    private $current_connections;
    
    /**
     * 私有构造方法，防止直接实例化
     */
    private function __construct() {}
    
    /**
     * 获取单例实例
     * @return MysqlPool
     */
    public static function getInstance()
    {
        // 使用进程ID作为键，确保每个Worker进程都有独立的实例
        $pid = getmypid();
        if (!isset(self::$instances[$pid])) {
            self::$instances[$pid] = new self();
        }
        return self::$instances[$pid];
    }
    
    /**
     * 初始化配置
     * @param array $config
     * @return MysqlPool
     */
    public function init($config = array())
    {
        $this->config = $config;
        $this->max_connections = isset($config['max_connections']) ? $config['max_connections'] : 10;
        // 初始化原子计数器
        $this->current_connections = new Swoole\Atomic(0);
        return $this;
    }
    
    /**
     * 获取MySQL连接
     * @return mysqli|null
     */
    public function getConnection()
    {
        $waitStart = microtime(true);
        $waitTimeout = 3; // 最大等待3秒

        while (true) {
            // 尝试从连接池中获取可用连接
            if (!empty($this->pool)) {
                $conn = array_pop($this->pool);
                $idle_in_pool = time() - $conn['time'];

                // 闲置超过30秒且当前连接数大于2，缩容释放
                if ($idle_in_pool > 30 && $this->current_connections->get() > 2) {
                    try {
                        $conn['conn']->close();
                    } catch (Exception $e) {}
                    $this->current_connections->sub(1);
                    continue; // 非递归，继续循环取下一个
                }

                // 检查连接是否有效
                if ($conn['conn']->ping()) {
                    return $conn['conn'];
                } else {
                    $conn['conn']->close();
                    $this->current_connections->sub(1);
                }
            }

            // 尝试创建新连接
            if ($this->current_connections->add(1) <= $this->max_connections) {
                $retryCount = 0;
                $maxRetries = 3;
                $retryDelay = 100000;

                while ($retryCount < $maxRetries) {
                    try {
                        $conn = new mysqli();
                        $conn->options(MYSQLI_OPT_CONNECT_TIMEOUT, $this->config['timeout'] ?? 2);
                        $conn->connect(
                            $this->config['host'],
                            $this->config['user'],
                            $this->config['password'],
                            $this->config['database'],
                            $this->config['port'] ?? 3306
                        );

                        if ($conn->connect_error) {
                            throw new Exception('MySQL connection error: ' . $conn->connect_error);
                        }

                        $conn->set_charset($this->config['charset'] ?? 'utf8mb4');
                        return $conn;
                    } catch (Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $this->current_connections->sub(1);
                            error_log('MySQL connection error after ' . $maxRetries . ' retries: ' . $e->getMessage());
                            return null;
                        }
                        usleep($retryDelay);
                    }
                }

                $this->current_connections->sub(1);
            } else {
                $this->current_connections->sub(1);
            }

            // 超时检查
            if (microtime(true) - $waitStart > $waitTimeout) {
                error_log('MySQL connection pool timeout. Current: ' . $this->current_connections->get() . ', Max: ' . $this->max_connections);
                return null;
            }

            usleep(10000); // 10ms
        }
    }
    
    /**
     * 归还MySQL连接到连接池
     * @param mysqli $conn
     */
    public function returnConnection($conn)
    {
        if ($conn instanceof mysqli) {
            // 为连接添加时间戳
            $this->pool[] = [
                'conn' => $conn,
                'time' => time()
            ];
        }
    }
    
    /**
     * 关闭所有连接
     */
    public function closeAll()
    {
        foreach ($this->pool as $conn) {
            if (isset($conn['conn']) && $conn['conn'] instanceof mysqli) {
                $conn['conn']->close();
            }
        }
        $this->pool = array();
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
    
    /**
     * 检查infohash是否存在
     * @param string|binary $infohash 二进制或十六进制字符串格式的infohash
     * @return bool
     */
    public function exists($infohash)
    {
        // 获取MySQL连接
        $conn = $this->getConnection();
        if (!$conn) {
            error_log('MySQL connection failed in exists() method');
            return false;
        }
        
        try {
            // 将二进制infohash转换为大写十六进制字符串
            $infohash_hex = strtoupper(bin2hex($infohash));
            
            // 生成完整表名
            $tableName = $this->config['prefix'] . $this->config['table_name'];
            
            // 准备查询语句
            $stmt = $conn->prepare("SELECT 1 FROM `{$tableName}` WHERE `infohash` = ? LIMIT 1");
            if (!$stmt) {
                throw new Exception('MySQL prepare error: ' . $conn->error);
            }
            
            $stmt->bind_param('s', $infohash_hex);
            $stmt->execute();
            $result = $stmt->get_result();
            
            // 检查结果
            $exists = $result->num_rows > 0;
            
            // 释放资源
            $stmt->close();
            $result->close();
            
            // 归还连接
            $this->returnConnection($conn);
            
            return $exists;
        } catch (Exception $e) {
            error_log('MySQL exists error: ' . $e->getMessage());
            // 归还连接
            $this->returnConnection($conn);
            return false;
        }
    }
}