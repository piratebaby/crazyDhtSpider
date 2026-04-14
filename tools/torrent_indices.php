<?php
/**
 * DHT Metadata 索引离线同步脚本
 * 功能：扫描 bt 表存量数据，填充 torrent_indices 索引表
 * 支持：Files优先、Name保底、正则清洗后缀、批量更新、进度统计
 */

error_reporting(E_ALL);
ini_set('display_errors', 1);
set_time_limit(0);
ini_set('memory_limit', '512M');

if (php_sapi_name() === 'cli') {
    if (ob_get_level() > 0) ob_end_clean();
    ob_implicit_flush(true);
}

echo "=== torrent_indices 离线同步脚本 ===\n";

// 从 dht_server 配置文件读取数据库配置，避免硬编码
// config.php 依赖 BASEPATH 常量，需要先定义
define('BASEPATH', dirname(__DIR__) . '/dht_server');
$config_file = BASEPATH . '/config.php';
if (!file_exists($config_file)) {
    die("错误: 找不到配置文件 {$config_file}\n");
}
$config = require $config_file;

if (empty($config['db']['host'])) {
    die("错误: 数据库配置为空，请先在 dht_server/config.php 中配置数据库连接信息\n");
}

$dbConfig = $config['db'];

try {
    echo "正在连接数据库 {$dbConfig['host']}...\n";
    $dsn = "mysql:host={$dbConfig['host']};port={$dbConfig['port']};dbname={$dbConfig['name']};charset=utf8mb4";
    $pdo = new PDO($dsn, $dbConfig['user'], $dbConfig['pass'], [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_TIMEOUT => 15,
        PDO::ATTR_EMULATE_PREPARES => false
    ]);
    echo "数据库连接成功！\n";
} catch (Exception $e) {
    die("连接失败: " . $e->getMessage() . "\n");
}

// 同步加载 dht_server 的 DbPool，复用其中的 analyzeTorrent 逻辑，确保一致性
$dbpool_file = dirname(__DIR__) . '/dht_server/inc/DbPool.class.php';
if (!file_exists($dbpool_file)) {
    die("错误: 找不到 {$dbpool_file}，无法同步分析逻辑\n");
}
require_once $dbpool_file;

// 参数配置
$batchSize = 500;        // 每批查询数量
$commitSize = 100;       // 每多少条提交一次事务（小事务，减少锁持有时间）

// 获取数据总量
try {
    $countStmt = $pdo->query("SELECT COUNT(*) FROM bt");
    $totalCount = (int)$countStmt->fetchColumn();
    $countStmt->closeCursor();
    echo "数据总量：{$totalCount} 条\n\n";
} catch (Exception $e) {
    die("获取数据总量失败: " . $e->getMessage() . "\n");
}

if ($totalCount === 0) {
    echo "没有需要处理的数据。\n";
    exit(0);
}

$lastInfohash = '';
$processedCount = 0;
$startTime = microtime(true);
$insertedCount = 0;
$updatedCount = 0;

$insertStmt = $pdo->prepare("INSERT INTO torrent_indices (infohash, file_category, primary_ext, total_size, file_count)
                             VALUES (?, ?, ?, ?, ?)
                             ON DUPLICATE KEY UPDATE
                                file_category = VALUES(file_category),
                                primary_ext = VALUES(primary_ext),
                                total_size = VALUES(total_size),
                                file_count = VALUES(file_count)");

echo "开始处理存量数据...\n";
echo str_repeat('-', 60) . "\n";

while (true) {
    $stmt = $pdo->prepare("SELECT infohash, name, files, length FROM bt WHERE infohash > ? ORDER BY infohash ASC LIMIT " . (int)$batchSize);
    $stmt->execute([$lastInfohash]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt->closeCursor();

    if (empty($rows)) {
        break;
    }

    $batchInserted = 0;
    $batchUpdated = 0;

    $pdo->beginTransaction();
    try {
        $batchCount = 0;

        foreach ($rows as $row) {
            $analysis = DbPool::analyzeTorrent($row['name'], $row['files']);

            $insertStmt->execute([
                $row['infohash'],
                $analysis['cat'],
                $analysis['ext'],
                $row['length'] ?? 0,
                $analysis['count']
            ]);

            // ON DUPLICATE KEY UPDATE: rowCount() 为1表示插入，为2表示更新
            if ($insertStmt->rowCount() === 1) {
                $insertedCount++;
                $batchInserted++;
            } else {
                $updatedCount++;
                $batchUpdated++;
            }

            $lastInfohash = $row['infohash'];
            $processedCount++;
            $batchCount++;

            // 每 $commitSize 条提交一次事务，避免长事务
            if ($batchCount >= $commitSize) {
                $pdo->commit();
                $pdo->beginTransaction();
                $batchCount = 0;
            }
        }

        $pdo->commit();
    } catch (Exception $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        echo "\n出错中止: " . $e->getMessage() . "\n";
        echo "已处理: {$processedCount} 条\n";
        exit(1);
    }

    // 输出进度
    $elapsed = round(microtime(true) - $startTime, 2);
    $speed = $elapsed > 0 ? round($processedCount / $elapsed, 0) : 0;
    $remaining = $speed > 0 ? round(($totalCount - $processedCount) / $speed, 2) : 0;
    $progress = min(100, round(($processedCount / $totalCount) * 100, 1));

    echo "[" . date('H:i:s') . "] 进度: {$progress}% | 已处理: {$processedCount}/{$totalCount} | "
       . "新增: {$batchInserted} 更新: {$batchUpdated} | "
       . "速度: {$speed}条/秒 | 剩余: {$remaining}秒\n";

    // 释放内存
    unset($rows);
    gc_collect_cycles();
}

// 完成统计
$totalTime = round(microtime(true) - $startTime, 2);
$avgSpeed = $totalTime > 0 ? round($processedCount / $totalTime, 0) : 0;

echo str_repeat('-', 60) . "\n";
echo "同步完成！\n";
echo "- 总数据量：{$totalCount} 条\n";
echo "- 已处理：{$processedCount} 条\n";
echo "- 新增：{$insertedCount} 条\n";
echo "- 更新：{$updatedCount} 条\n";
echo "- 总耗时：{$totalTime} 秒\n";
echo "- 平均速度：{$avgSpeed} 条/秒\n";
