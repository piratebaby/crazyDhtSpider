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
        // 尝试从连接池中获取可用连接
        if (!empty($this->pool)) {
            $conn = array_pop($this->pool);
            // 【新增：池化缩容逻辑】解决连接无法释放的问题
            $idle_in_pool = time() - $conn['time']; // 计算在池子里的发呆时间
            
            // 如果在池子里闲置超过 30 秒，且当前 Worker 拥有的连接数大于 2 个（保留最低水位）
            if ($idle_in_pool > 30 && $this->current_connections->get() > 2) {
                try {
                    $conn['conn']->close(); // 主动切断，让它安息
                } catch (Exception $e) {}
                
                $this->current_connections->sub(1); // 计数器减 1
                // 重新尝试获取连接
                return $this->getConnection();
            }
            
            // 检查连接是否有效
            if ($conn['conn']->ping()) {
                return $conn['conn'];
            } else {
                // 连接无效，关闭并减少计数
                $conn['conn']->close();
                $this->current_connections->sub(1);
            }
        }
        
        // 检查是否达到最大连接数，使用原子操作避免并发问题
        if ($this->current_connections->add(1) <= $this->max_connections) {
            // 连接重试机制
            $retryCount = 0;
            $maxRetries = 3;
            $retryDelay = 100000; // 100毫秒
            
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
                        error_log('MySQL connection error after ' . $maxRetries . ' retries: ' . $e->getMessage());
                        // 创建连接失败，减少计数
                        $this->current_connections->sub(1);
                        return null;
                    }
                    error_log('MySQL connection attempt ' . $retryCount . ' failed, retrying in ' . ($retryDelay / 1000) . 'ms: ' . $e->getMessage());
                    usleep($retryDelay);
                }
            }
            
            // 所有重试都失败，减少计数
            $this->current_connections->sub(1);
        } else {
            // 连接数达到上限，减少计数
            $this->current_connections->sub(1);
            error_log('MySQL connection pool exhausted. Current: ' . $this->current_connections->get() . ', Max: ' . $this->max_connections);
        }
        
        return null;
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