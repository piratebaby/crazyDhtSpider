<?php
/**
 * 大数据量infohash导入脚本
 * 功能：将MySQL history表中的infohash批量导入到Redis
 * 支持：200万+数据量，内存占用低，速度快
 */

error_reporting(E_ALL);
ini_set('display_errors', 1);
set_time_limit(0);
ini_set('memory_limit', '512M');

echo "=== DHT infohash大数据量导入工具 ===\n";

// 从 dht_server 配置文件读取数据库配置，避免硬编码
define('BASEPATH', dirname(__DIR__) . '/dht_server');
$config_file = BASEPATH . '/config.php';
if (!file_exists($config_file)) {
    die("错误: 找不到配置文件 {$config_file}\n");
}
$db_config_source = require $config_file;

if (empty($db_config_source['db']['host'])) {
    die("错误: 数据库配置为空，请先在 dht_server/config.php 中配置数据库连接信息\n");
}

// 数据库配置（从统一配置读取）
$db_config = [
    'host' => $db_config_source['db']['host'],
    'port' => $db_config_source['db']['port'] ?? 3306,
    'user' => $db_config_source['db']['user'],
    'pass' => $db_config_source['db']['pass'],
    'dbname' => $db_config_source['db']['name'],
    'charset' => 'utf8mb4'
];

// Redis配置（从 dht_client/config.php 读取，保持与运行时一致）
$client_config_file = dirname(__DIR__) . '/dht_client/config.php';
if (file_exists($client_config_file)) {
    $client_config = require $client_config_file;
    $redis_source = $client_config['redis'] ?? [];
} else {
    $redis_source = [];
}

$redis_config = [
    'host' => $redis_source['host'] ?? 'localhost',
    'port' => $redis_source['port'] ?? 6379,
    'password' => $redis_source['password'] ?? '',
    'database' => $redis_source['database'] ?? 0,
    'timeout' => $redis_source['timeout'] ?? 2
];

// 导入参数配置
$import_config = [
    'batch_size' => 10000,                     // 每批处理数量（推荐5000-20000）
    'table_name' => 'history',                 // 源数据表名
    'infohash_field' => 'infohash',            // infohash字段名
    'expire_time' => $redis_source['infohash_expire'] ?? 86400,  // Redis数据过期时间（秒）
    'prefix' => $redis_source['prefix'] ?? 'dht_'               // Redis键前缀
];

// 输出配置信息
echo "导入配置：\n";
echo "- 数据库：{$db_config['host']}:{$db_config['port']}/{$db_config['dbname']}\n";
echo "- 源表：{$import_config['table_name']}.{$import_config['infohash_field']}\n";
echo "- Redis：{$redis_config['host']}:{$redis_config['port']}\n";
echo "- 每批处理：{$import_config['batch_size']} 条\n";
echo "- 过期时间：{$import_config['expire_time']} 秒\n";
echo "- 键前缀：{$import_config['prefix']}\n";
echo str_repeat('=', 50) . "\n\n";

// 连接MySQL
try {
    $pdo = new PDO(
        "mysql:host={$db_config['host']};port={$db_config['port']};dbname={$db_config['dbname']};charset={$db_config['charset']}",
        $db_config['user'],
        $db_config['pass'],
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
            PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true
        ]
    );
    echo "MySQL连接成功\n";
} catch (PDOException $e) {
    die("MySQL连接失败：" . $e->getMessage() . "\n");
}

// 连接Redis
try {
    $redis = new Redis();
    $redis->connect($redis_config['host'], $redis_config['port'], $redis_config['timeout']);

    if (!empty($redis_config['password'])) {
        $redis->auth($redis_config['password']);
    }

    if (!empty($redis_config['database'])) {
        $redis->select($redis_config['database']);
    }

    echo "Redis连接成功\n";
} catch (RedisException $e) {
    die("Redis连接失败：" . $e->getMessage() . "\n");
}

// 获取数据总量
try {
    $count_stmt = $pdo->prepare("SELECT COUNT(*) FROM {$import_config['table_name']}");
    $count_stmt->execute();
    $total_count = (int)$count_stmt->fetchColumn();
    $count_stmt->closeCursor();
    echo "数据总量：{$total_count} 条\n";
} catch (PDOException $e) {
    die("获取数据总量失败：" . $e->getMessage() . "\n");
}

if ($total_count === 0) {
    echo "没有需要导入的数据。\n";
    $pdo = null;
    $redis->close();
    exit(0);
}

// 开始导入
echo "\n开始导入数据...\n";
echo str_repeat('-', 50) . "\n";

$processed_count = 0;
$start_time = microtime(true);
$last_output_time = $start_time;
$last_infohash = '';
$set_key = $import_config['prefix'] . 'infohashes';

try {
    while (true) {
        // 重置Redis连接
        if (!$redis->isConnected()) {
            $redis->connect($redis_config['host'], $redis_config['port'], $redis_config['timeout']);
        }

        // 基于主键的分页查询，避免OFFSET性能问题
        if ($last_infohash === '') {
            $stmt = $pdo->prepare("SELECT {$import_config['infohash_field']} FROM {$import_config['table_name']} ORDER BY {$import_config['infohash_field']} LIMIT {$import_config['batch_size']}");
        } else {
            $stmt = $pdo->prepare("SELECT {$import_config['infohash_field']} FROM {$import_config['table_name']} WHERE {$import_config['infohash_field']} > ? ORDER BY {$import_config['infohash_field']} LIMIT {$import_config['batch_size']}");
            $stmt->bindValue(1, $last_infohash, PDO::PARAM_STR);
        }

        $stmt->execute();
        $batch_data = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        if (empty($batch_data)) {
            break;
        }

        // 开启Redis管道，批量写入
        $redis->multi(Redis::PIPELINE);
        foreach ($batch_data as $row) {
            $infohash_hex = $row[$import_config['infohash_field']];
            $infohash_bin = hex2bin($infohash_hex);
            $redis->sAdd($set_key, $infohash_bin);
        }
        $redis->exec();

        // 更新统计
        $processed_count += count($batch_data);
        $last_infohash = end($batch_data)[$import_config['infohash_field']];

        // 释放内存（无条件释放）
        unset($batch_data);

        // 输出进度（每10秒输出一次）
        $now = microtime(true);
        if (($now - $last_output_time) >= 10 || $processed_count >= $total_count) {
            $last_output_time = $now;
            $elapsed = round($now - $start_time, 2);
            $speed = $elapsed > 0 ? round($processed_count / $elapsed, 0) : 0;
            $remaining = $speed > 0 ? round(($total_count - $processed_count) / $speed, 2) : 0;
            $progress = min(100, round(($processed_count / $total_count) * 100, 1));

            echo "[" . date('H:i:s') . "] 进度: {$progress}% | 已处理: {$processed_count}/{$total_count} | 速度: {$speed}条/秒 | 剩余: {$remaining}秒\n";
        }

        gc_collect_cycles();
    }

    // 循环结束后统一设置过期时间（不再每批次重复设置）
    $redis->expire($set_key, $import_config['expire_time']);

} catch (PDOException $e) {
    die("\n数据导入失败（MySQL错误）：" . $e->getMessage() . "\n");
} catch (RedisException $e) {
    die("\n数据导入失败（Redis错误）：" . $e->getMessage() . "\n");
} catch (Throwable $e) {
    die("\n数据导入失败（系统错误）：" . $e->getMessage() . "\n");
}

// 完成导入
$total_time = round(microtime(true) - $start_time, 2);
$avg_speed = $total_time > 0 ? round($processed_count / $total_time, 0) : 0;

// 关闭连接
$pdo = null;
$redis->close();

// 输出完成信息
echo str_repeat('-', 50) . "\n";
echo "导入完成！\n";
echo "- 总数据量：{$total_count} 条\n";
echo "- 成功导入：{$processed_count} 条\n";
echo "- 总耗时：{$total_time} 秒\n";
echo "- 平均速度：{$avg_speed} 条/秒\n";
echo "- Redis键：{$set_key}\n";
echo "- 数据过期时间：{$import_config['expire_time']} 秒\n";
echo "\n提示：可使用以下命令验证Redis数据：\n";
echo "   redis-cli SCARD '{$set_key}'\n";
echo "   redis-cli TTL '{$set_key}'\n";
