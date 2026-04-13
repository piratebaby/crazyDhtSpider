<?php

class DbPool
{
    private static $instance = null;
    private $pool = []; // 连接池数组
    private $maxConnections = 200; // 最大连接数
    private $connectionsCount = 0; // 当前连接数
    private $waitTimeout = 3; // 等待连接超时时间（秒）

    private function __construct() {}

    public static function getInstance(): DbPool
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function createConnection(): PDO
    {
        global $config;

        $dsn = "mysql:host={$config['db']['host']};dbname={$config['db']['name']};charset=utf8mb4";
        $options = [
            PDO::ATTR_PERSISTENT => false,
            PDO::ATTR_TIMEOUT => 5,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
            PDO::ATTR_EMULATE_PREPARES => false
        ];

        $pdo = new PDO($dsn, $config['db']['user'], $config['db']['pass'], $options);
        $this->connectionsCount++;
        return $pdo;
    }

    /**
     * 从连接池获取连接（非递归，循环等待）
     */
    private function getConnection(): ?PDO
    {
        $waitStart = microtime(true);

        while (true) {
            // 优先从连接池中获取可用连接
            while (!empty($this->pool)) {
                $connection = array_pop($this->pool);

                // 快速验证连接（只对空闲超过30秒的连接做SELECT 1）
                if ($connection['idle_time'] > 30) {
                    try {
                        $connection['pdo']->query('SELECT 1');
                    } catch (Exception $e) {
                        $this->connectionsCount--;
                        continue;
                    }
                }

                return $connection['pdo'];
            }

            // 连接池为空且未达到最大连接数，创建新连接
            if ($this->connectionsCount < $this->maxConnections) {
                try {
                    return $this->createConnection();
                } catch (Exception $e) {
                    error_log("Database connection creation failed: " . $e->getMessage());
                    return null;
                }
            }

            // 超时检查
            if (microtime(true) - $waitStart > $this->waitTimeout) {
                error_log("Database connection pool timeout after {$this->waitTimeout}s");
                return null;
            }

            // 短暂等待后重试
            usleep(10000); // 10ms
        }
    }

    private function releaseConnection(PDO $pdo): void
    {
        $this->pool[] = ['pdo' => $pdo, 'idle_time' => 0];
    }

    /**
     * 核心分析逻辑：计算资源分类与主体后缀
     */
    private static function analyzeTorrent($name, $filesJson): array
    {
        $extToCategory = [
            'mp4'=>'Video', 'mkv'=>'Video', 'avi'=>'Video', 'rmvb'=>'Video', 'ts'=>'Video', 'mov'=>'Video', 'wmv'=>'Video', 'flv'=>'Video',
            'zip'=>'Archive', 'rar'=>'Archive', '7z'=>'Archive', 'tar'=>'Archive', 'iso'=>'DiskImage',
            'mp3'=>'Audio', 'flac'=>'Audio', 'wav'=>'Audio', 'ape'=>'Audio',
            'pdf'=>'Document', 'epub'=>'Document', 'mobi'=>'Document', 'txt'=>'Text',
            'exe'=>'Software', 'apk'=>'Software', 'dmg'=>'Software', 'pkg'=>'Software'
        ];

        $extSizeMap = [];
        $files = json_decode($filesJson, true);
        $count = 0;

        if ($files && is_array($files)) {
            $count = count($files);
            foreach ($files as $file) {
                $path = $file['path'] ?? '';
                $length = $file['length'] ?? 0;
                $filename = is_array($path) ? end($path) : $path;
                $ext = strtolower(pathinfo($filename, PATHINFO_EXTENSION));

                if (empty($ext) || $ext == 'pad' || strpos($filename, '_____padding_file_') === 0) continue;

                if (!isset($extSizeMap[$ext])) $extSizeMap[$ext] = 0;
                $extSizeMap[$ext] += $length;
            }
        }

        if (empty($extSizeMap)) {
            $count = ($count > 0) ? $count : 1;
            $ext = strtolower(pathinfo($name, PATHINFO_EXTENSION));
            if (!empty($ext)) $extSizeMap[$ext] = 1;
        }

        if (empty($extSizeMap)) {
            return ['ext' => 'unknown', 'cat' => 'Other', 'count' => $count];
        }

        arsort($extSizeMap);
        $primaryExt = array_key_first($extSizeMap);
        $primaryExt = substr($primaryExt, 0, 30);
        $category = $extToCategory[$primaryExt] ?? 'Other';

        return ['ext' => $primaryExt, 'cat' => $category, 'count' => $count];
    }

    /**
     * 执行数据库查询（带事务）
     */
    public static function sourceQuery($rs, $bt_data): void
    {
        $instance = self::getInstance();
        $pdo = $instance->getConnection();

        if ($pdo === null) {
            error_log("Failed to get database connection");
            return;
        }

        $stmt = null;

        try {
            $pdo->beginTransaction();

            // 使用 INSERT IGNORE 原子性插入 history 表，避免并发竞态导致的重复键错误
            // INSERT IGNORE: 如果主键冲突则静默跳过，不报错
            $stmt = $pdo->prepare("INSERT IGNORE INTO history (infohash) VALUES (?)");
            $stmt->execute([$rs['infohash']]);
            $inserted = $stmt->rowCount() > 0;
            $stmt->closeCursor();

            if ($inserted) {
                // 新记录：插入 bt 表
                $bt_data['time'] = 'NOW()';
                $bt_data['lasttime'] = 'NOW()';

                $columns = [];
                $placeholders = [];
                $values = [];

                foreach ($bt_data as $column => $value) {
                    $columns[] = $column;
                    if ($value === 'NOW()') {
                        $placeholders[] = 'NOW()';
                    } else {
                        $placeholders[] = '?';
                        $values[] = $value;
                    }
                }

                $columns_str = implode(', ', $columns);
                $placeholders_str = implode(', ', $placeholders);
                $sql = "INSERT INTO bt ({$columns_str}) VALUES ({$placeholders_str})";

                $stmt = $pdo->prepare($sql);
                $stmt->execute($values);
                $stmt->closeCursor();

                // 同步插入到 torrent_indices 索引表
                $analysis = self::analyzeTorrent($bt_data['name'] ?? '', $bt_data['files'] ?? '[]');

                $idx_sql = "INSERT INTO torrent_indices (infohash, file_category, primary_ext, total_size, file_count)
                            VALUES (?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                                file_category = VALUES(file_category),
                                primary_ext = VALUES(primary_ext),
                                total_size = VALUES(total_size),
                                file_count = VALUES(file_count)";

                $stmt = $pdo->prepare($idx_sql);
                $stmt->execute([
                    $rs['infohash'],
                    $analysis['cat'],
                    $analysis['ext'],
                    $bt_data['length'] ?? 0,
                    $analysis['count']
                ]);
                $stmt->closeCursor();
            } else {
                // 已存在：只增加热度
                $stmt = $pdo->prepare("UPDATE bt SET hot = hot + 1, lasttime = NOW() WHERE infohash = ?");
                $stmt->execute([$rs['infohash']]);
                $stmt->closeCursor();
            }

            $pdo->commit();
            $instance->releaseConnection($pdo);
        } catch (Exception $e) {
            if (isset($pdo) && $pdo->inTransaction()) {
                $pdo->rollBack();
            }
            if ($stmt !== null) {
                try { $stmt->closeCursor(); } catch (Exception $e2) {}
            }
            // 出错的连接不放回池中
            $instance->connectionsCount--;
            error_log("Database query error: " . $e->getMessage());
        }
    }

    public static function setMaxConnections(int $max): void
    {
        $instance = self::getInstance();
        $instance->maxConnections = $max;
    }

    public static function close(): void
    {
        $instance = self::getInstance();
        $instance->pool = [];
        $instance->connectionsCount = 0;
    }

    /**
     * 更新空闲时间（由定时器调用）
     */
    public static function tickIdleTime(): void
    {
        $instance = self::getInstance();
        foreach ($instance->pool as $key => $conn) {
            $instance->pool[$key]['idle_time'] += 10;
        }
    }
}
