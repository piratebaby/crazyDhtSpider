<?php
/*
 * 设置服务器 ulimit -n 65535
 * 记得放开防火墙2345端口
 */
define('BASEPATH', dirname(__FILE__));
define('DEBUG', false);
$config = require_once BASEPATH . '/config.php';
require_once BASEPATH . '/inc/Func.class.php';
require_once BASEPATH . '/inc/DbPool.class.php';
require_once BASEPATH . '/inc/Bencode.class.php';
require_once BASEPATH . '/inc/MySwoole.class.php';
require_once "vendor/autoload.php";

Func::Logs(date('Y-m-d H:i:s', time()) . " - 服务启动..." . PHP_EOL, 1);
$serv = new Swoole\Server('0.0.0.0', 2345, SWOOLE_PROCESS, SWOOLE_SOCK_UDP);
// 额外添加一个 IPv6 监听端口，支持dht_client通过IPv6发送metadata数据
$serv->addListener('::', 2345, SWOOLE_SOCK_UDP6);
$serv->set($config['server']);
Swoole\Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

// 注册启动事件，设置主进程名称
$serv->on('start', function ($serv) {
    // 设置主进程名称
    swoole_set_process_name("php_dht_server_master");
});

$serv->on('WorkerStart', 'MySwoole::workStart');
$serv->on('Packet', 'MySwoole::packet');
$serv->on('WorkerExit', 'MySwoole::workerExit');
$serv->start();
