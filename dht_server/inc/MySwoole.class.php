<?php

class MySwoole
{
    public static function workStart($serv, $worker_id)
    {
        global $config;
        swoole_set_process_name("php_dht_server_event_worker");

        // 添加内存管理定时器（每30秒执行一次，从10秒放宽）
        swoole_timer_tick(30000, function ($timer_id) use ($serv) {
            // 执行垃圾回收
            gc_mem_caches();
            gc_collect_cycles();

            // 更新数据库连接池空闲时间
            DbPool::tickIdleTime();

            // 记录服务器状态和内存使用情况
            $stats = $serv->stats();
            $memory = memory_get_usage(true);
            Func::Logs(json_encode($stats) . " | Memory: " . Func::sizecount($memory) . PHP_EOL, 3);
        });

        if (!DEBUG) {
            try {
                DbPool::getInstance();
            } catch (Exception $e) {
                Func::Logs("数据库连接失败: " . $e->getMessage() . PHP_EOL);
            }
        }
    }

    public static function packet($serv, $data, $fdinfo)
    {
        if (strlen($data) == 0) {
            return false;
        }

        try {
            $rs = Base::decode($data);
            if (!is_array($rs) || !isset($rs['infohash'])) {
                return false;
            }

            if (empty(Func::getBtFiles($rs))) {
                return false;
            }

            $rs = Func::getBtFiles($rs);
            $bt_data = Func::getBtData($rs);

            if (DEBUG) {
                Func::Logs(json_encode($bt_data, JSON_UNESCAPED_UNICODE) . PHP_EOL, 2);
                return false;
            }

            // 使用协程处理数据库操作
            \Swoole\Coroutine::create(function () use ($bt_data, $rs) {
                try {
                    DbPool::sourceQuery($rs, $bt_data);
                } catch (Exception $e) {
                    error_log("数据插入失败: " . $e->getMessage());
                }
            });

        } catch (Exception $e) {
            error_log("Packet处理失败: " . $e->getMessage());
        }

        return true;
    }

    public static function task(Swoole\Server $serv, Swoole\Server\Task $task)
    {
    }

    public static function workerExit($serv, $worker_id)
    {
        Swoole\Timer::clearAll();

        DbPool::close();

        gc_mem_caches();
        gc_collect_cycles();
    }

    public static function finish($serv, $task_id, $data)
    {
    }
}
