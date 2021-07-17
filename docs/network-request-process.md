# 一、综述

  kvrocks的网络请求处理框架与redis的类似，同样都采用reactor模式设计，不同之处在于并发访问的实现。redis自身封装了多种I/O多路复用技术来实现不同平台上的并发访问，而kvrocks使用libevent库实现。redis是单线程实现并发访问，而kvrocks是多线程实现，kvrocks支持同时启动多个worker，每个worker是一个线程，每个worker独立使用libevent处理网络事件。

## 二、kvrocks网络请求处理框架

  kvrocks网络请求处理框架可以描述为图2.1所示结构，主要可以分为两个层次：网络事件处理层、请求执行层。

![kvrock______](/Users/zoumingfo/Downloads/kvrock______.png)

<center>图2.1 kvrocks请求处理框架</center>

### 2.1 网络事件处理层

  网络事件处理层由多个worker线程组成，并发的处理客户端与kvrocks之间的网络事件。

  kvrocks创建多个worker线程来处理网络请求，默认为8个worker；每个worker都监听一个socket，使用内核的IP/port复用机制，所有socket都绑定在相同的IP/port上；多个线程同时监听处理网络请求，由操作系统内核实现请求在多个worker间的均衡，实现kvrocks的client建链的并发访问。

  每个worker accept的连接由该worker进行维护，每条连接上网络事件都在同一个worker线程中进行处理。也就是说，worker线程同时负责监听事件处理，还负责请求事件的处理。网络事件的并发处理由libevent实现，libevent有关的内容参见文末附链接。

### 2.2 请求执行层

  执行层即为各数据类型的实现，kvrocks支持string/hash/set/zset/list/bitmap/sortint共7种key类型。请求处理通过解析请求、查询这些命令实现，获取到正确的命令并执行，实现对应数据类型的读取、写入等操作。

## 三、业界多线程模型对比

### 3.1 memcached

  memcached网络处理模型如图3.1所示，memcached是多线程模型，分主线程、worker线程池子，由主线程监听外部连接请求，当accept请求后，就交给某一个worker线程进行后续连接的事件处理。worker线程与主线程之间交互通过pipe实现，即worker线程同时处理与主线程之间的pipe上的事件处理，也处理worker自身负责的连接的网络事件处理。

<img src="/Users/zoumingfo/Library/Application Support/typora-user-images/image-20210630172031708.png" alt="image-20210630172031708" style="zoom:40%;" />

<center>图3.1 memcached网络处理模型</center>

### 3.2 tendis

  tendis网络处理模型如下图3.2所示，与memcached类似，都拆分出监听线程负责监听外部连接请求，以及工作线程池进行连接事件处理。不同之处在于将网络I/O与事件处理进行了进一步拆分，分为网络I/O线程池与worker线程池，分别处理网络I/O与实际的请求处理。两个线程池之间通过工作队列与Response队列进行通信。

![image-20210630171939065](/Users/zoumingfo/Library/Application Support/typora-user-images/image-20210630171939065.png)

<center>图3.2 tendis网络处理模型</center>

### 3.3 kvrocks

kvrocks与以上两者之间的主要异同：

1. kvrocks与以上两者的实现相同之处在于都是采用多线程模型实现并发；
2. 主要不同在于kvrocks建链请求是由多个worker线程同时进行，worker线程还同时负责管理由其建立的连接，连接上的请求处理也在同一个worker中进行；
3. kvrocks不拆分监听线程与请求处理线程，不需要维护工作队列，也不用维护多个线程池，在实现高效的并发的同时架构更简单易于理解；



# 附：

libevent源码： https://github.com/libevent/libevent

libevent解析参考：https://aceld.gitbooks.io/libevent/content/

