# 整体流程

1. 加载 Kvrocks 配置(解析配置文件，构建 `Config` 对象)
2. 初始化并打开存储引擎 `Engine::Storage`
3. 初始化服务器 `Server`
4. 运行 `Server`，执行 `Server::Start()` 和 `Server::Join()`
5. 接收中断信号，终止 `Server`
   
   调用路径: 中断信号处理函数 `signal_handler` ->  `hup_handler` -> `Server::Stop()`

# 存储引擎 

涉及到的文件:

- storage.h, storage.cc

## Engine::Storage

封装底层存储引擎（目前使用的是 `RocksDB` ）的接口, 为 Server 提供数据存储接口。
涉及到的 `RocksDB` 类如下: 

- `rocksdb::DB`
- `rocksdb::BackupEngine`
- `rocksdb::Env`
- `rocksdb::SstFileManager`
- `rocksdb::RateLimiter`
- `rocksdb::ColumnFamilyHandle`

## Storage::Open

1. 创建需要的 `ColumnFamily`
2. 配置每个 `ColumnFamily`
3. 调用 `rocksdb::DB::Open()` 打开 RocksDB，并统计用时
4. 调用 `rocksdb::BackupEngine::Open()` 打开 `BackupEngine`

# 服务器

涉及到的文件: 

- server.h, server.cc
- redis_cmd.cc
- worker.h, worker.cc

## Server 初始化

1. 调用 `Redis::GetCommandList` 函数获得命令表，并初始化命令统计
2. 创建 `Worker` 和 `WorkerThread`， 工作线程（可以配置数目），用于处理请求
3. 构建主从复制 Worker （用于主从同步），可以配置数目，也可以设置限速

## Server::Start()

1. 启动 工作线程 和 复制线程， `WorkerThread.Start()`
2. 启动 `TaskRunner`， 用于处理异步任务 `Task`
3. 构建并启动一个 Cron 线程
4. 构建并启动一个 `CompactionChecker` 线程，定时手动进行 `RocksDB` 的Compaction  

# 线程模型

Kvrocks 包括 Worker 线程、TaskRunner 线程池、Cron 周期线程、CompactionChecker 线程、主从复制线程。

## Worker线程

### Worker

Kvrocks 使用 `libevent` 库进行事件处理。

涉及到的文件:

- worker.h, worker.cc

工作线程 `Worker` 和 `Redis::Connection` 绑定在一起，内部用 map 存储 fd 和 Connection 的映射关系，成员函数主要是连接相关的逻辑以及event相关的对象。

初始化 `Worker`（构造函数）:
- `event_base_new()` 创建 `event_base`
- 创建 `timer event` （默认10s检查一次）回调函数 `Worker::TimerCB()`
- 调用 `listen()` 监听端口
    - 监听到事件之后，将返回的 `evconnlistener` 添加到监听事件列表 `listen_events_`上，回调函数是 `Worker::newConnection()`

`Worker::TimerCB()`: 定时器回调函数
- 检查超时的 client，从 `Worker` 维护的相关数据结构中踢出

`Worker::Run()` :
- `event_base_dispatch` 启动 `event_base` 的事件循环， 处理就绪的事件

`Worker::newConnection()` :
- 获取 `bufferevent`
- 创建 `Redis::Connection(bev, worker)`
- 设置 `bufferevent` 的读、写、事件三种回调函数，分别为: `Redis::Connection::OnRead()`、`Redis::Connection::OnWrite()`、`Redis::Connection::OnEvent()`
- 将 `Redis::Connection` 添加到此 `Worker` 的 `map<int, Redis::Connection*> conns_` 中
- 设置复制 `Worker` 的限速

### WorkerThread

涉及到的文件:

- worker.h, worker.cc

`WorkerThread`: 将 thread 和 Worker 封装在一起

`WorkerThread.Start()`:

1. 构建并启动 thread
2. thread 中执行 `Worker::Run()`

### Redis::Connection

涉及到的文件:

- redis_connection.h, redis_connection.cc

将客户端的连接抽象为 `Connection`，并将一系列操作封装其中，内部使用 `libevent` 的 `eventbuffer` 做数据的读取和写入。

bufferevent

每个连接的socket上面会有数据，数据将存在 bufferevent 的缓冲区上，对于 bufferevent 的三种回调函数:
- 当输入缓冲区的数据大于等于输入低水位时，读取回调就会被调用。默认情况下，输入低水位的值是 0，也就是说，只要 socket 变得可读，就会调用读取回调
- 当输出缓冲区的数据小于等于输出低水位时，写入回调就会被调用。默认情况下，输出低水位的值是 0，也就是说，只有当输出缓冲区的数据都发送完了，才会调用写入回调。因此，默认情况下的写入回调也可以理解成为写完成了，就会调用
- 连接关闭、连接超时或者连接发生错误时，则会调用事件回调

`Connection::OnRead()`: 读取数据，查找对应命令列表，然后执行
- 调用 `Connection::Input()`， 读取 bufferevent 中的内容
- 调用 `Request::Tokenize` 将命令解析成 Token 保存在 `Request` 内部
- 调用 `Connection::ExecuteCommands`，执行命令

`Connection::OnWrite()`: 回复客户端完毕
- `Connection::Close()`，内部调用 `Worker::FreeConnection`

`Connection::OnEvent()`: 处理出错、连接关闭、超时情况

`Connection::ExecuteCommands()`:
- 调用 `Server::LookupAndCreateCommand` 查找命令表获得命令
- 判断命令是否合法
- 如果命令合法，判断命令的参数是否合法
    - 数目是否合法
    - 参数类型（等其他方面是否合法），调用每个命令的 Parse 函数（每个命令都会重写基类 `Commander` 的 `Parse` 函数）
- 调用当前命令的 `Execute` 函数执行当前命令，获得回复字符串
- 统计命令的执行时间
- 处理 `monitor` 命令的逻辑: 调用 `Server::FeedMonitorConns`，将当前命令发送给 Monitor 客户端连接
- `Connection::Reply()`: 调用 `Redis::Reply()` 将响应写入 `bufferevent` 回复给客户端

### Redis::Request

涉及到的文件:

- redis_request.h, redis_request.cc

主要用来解析 eventbuffer 中的数据， 解析成 Redis 命令，并执行

`Request::Tokenize()`: 

将客户端传来的数据从 `eventbuffer` 中读出，分隔成 Token

## TaskRunner 线程池

涉及到的文件:

- task_runner.h, task_runner.cc

是一个线程池，有任务队列，用来存储异步的任务( Task )，当前异步的任务有:

- `Server::AsyncCompactDB()`: `compact` 命令、`Server::cron` 调用
- `Server::AsyncBgsaveDB()`: `bgsave` 命令、`Server::cron` 调用
- `Server::AsyncPurgeOldBackups()`:  `flushbackup` 命令、`Server::cron` 调用
- `Server::AsyncScanDBSize()`:  `dbsize` 命令调用

可以发现这些都是比较耗时的任务， 为了不阻塞其他请求。

TaskRunner::Start():
- 创建线程执行 TaskRunner::run
- TaskRunner::run 无限循环， 执行队列中的 Task

## Cron 周期线程

相关文件:
- server.h, server.cc

Server的周期函数，执行一些定时任务(100ms是一个时钟嘀嗒):
- `AsyncCompactDB` 周期 20s 
- `AsyncBgsaveDB` 周期 20s
- `AsyncPurgeOldBackups` 周期 1min
- `autoResizeBlockAndSST` 动态改变RocksDB的参数 `target_file_size_base` 和 `write_buffer_size` 的大小，周期 30min
- `Server::cleanupExitedSlaves()`

## CompactionChecker 清理线程

相关文件:
- compaction_checker.h, compaction_checker.cc

每 1min 检查一次， `CompactionChecker` 中有 `CompactPubsubAndSlotFiles` 和
`PickCompactionFiles` 两个函数:
- `CompactPubsubAndSlotFiles`: 清理 pubsub 相关的 ColumnFamily
- `PickCompactionFiles`: 获取 SST 文件的 `TableProperties`，其中包含了 SST 的属性: 

    1. key 的总数目 
    2. 删除 key 的数目 
    3. 起始 key 
    4. 终止 key
    
    对满足以下条件的 SST 文件进行手动 Compaction:

    1. 创建超过两天的 SST 文件  
    2. 删除key占比多的 SST 文件

获取 SST 属性

`CompactOnExpiredCollector` 通过继承 `rocksdb::TablePropertiesCollector` 实现自定义 SST 属性，然后实现相应的工厂类 `CompactOnExpiredTableCollectorFactory`，将工厂类通过 `rocksdb::ColumnFamilyOptions` 传递给存储引擎。

## 主从复制线程

执行主从复制相关的逻辑，具体见: [replication-design](./replication-design.md)

# 命令实现

## 编码

将Redis相关命令编码成KV数据，具体见: [metadata-design](./metadata-design.md)

## 实现

按照编码规则，使用 redis_xx.h 中定义的数据结构，构造编码后的KV数据，最后使用存储引擎 `Engine::Storage` 封装的接口将最终KV数据保存。
