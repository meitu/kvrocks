<img src="docs/images/kvrocks_logo.png" alt="kvrocks_logo" width="350"/>

# ![image](https://github.com/kvrockslabs/kvrocks/workflows/kvrocks%20ci%20actions/badge.svg) ![image](https://img.shields.io/badge/build-passing-brightgreen) [![GitHub license](https://img.shields.io/github/license/kvrockslabs/kvrocks)](https://github.com/kvrockslabs/kvrocks/blob/unstable/LICENSE) [![GitHub stars](https://img.shields.io/github/stars/kvrockslabs/kvrocks)](https://github.com/kvrockslabs/kvrocks/stargazers)

- [Google Group](https://groups.google.com/g/kvrocks)
- [Slack Channel](https://join.slack.com/t/kvrockscommunity/shared_invite/zt-p5928e3r-OUAK8SUgC8GOceGM6dAz6w)

Kvrocks is an open-source key-value database which is based on rocksdb and compatible with Redis protocol. Intention to decrease the cost of memory and increase the capability while compared to Redis. The design of replication and storage was inspired by `rocksplicator` and `blackwidow`.

Kvrocks has the following key features:

- Redis protocol, user can use redis client to visit the kvrocks
- Namespace, similar to redis db but use token per namespace
- Replication, async replication using binlog like MySQL
- High Available, supports redis sentinel to failover when master or slave was failed

> Thanks for designers @[田凌宇](https://github.com/tianlingyu1997) and @范世丽 contribute the kvrocks logo for us.

## Who uses kvrocks 

<table>
<tr>
<td height = "128" width = "164"><img src="https://imgur.com/9X1kc2j.png" alt="Meitu"></td>
<td height = "128" width = "164"><img src="https://imgur.com/vqgSmMz.jpeg" alt="Ctrip"></td>
</tr>
<tr>
<td height = "128" width = "164"><img src="docs/images/baidu_logo.png" alt="Baidu"></td>
<td height = "128" width = "164"><img src="https://imgur.com/MJsoEN7.png" alt="BaishanCloud"></td>
</tr>
</table>

***Tickets a pull reqeust to let us known that you're using kvrocks and add your logo to README***

## Building kvrocks

#### requirements

* g++ (required by c++11, version >= 4.8)
* autoconf automake libtool snappy

#### Build

***NOTE: You should install the snappy first:***

```shell
# Centos/Redhat
sudo yum install -y epel-release && sudo yum install -y git gcc gcc-c++ make snappy snappy-devel autoconf automake libtool which gtest gtest-devel

# Ubuntu
sudo apt-get install gcc g++ make libsnappy-dev autoconf automake libtool which libgtest-dev

# MACOSX
brew install snappy googletest
```

It is as simple as:

```shell
$ git clone --recursive https://github.com/kvrockslabs/kvrocks.git
$ cd kvrocks
$ make -j4
```

### Running kvrocks

```shell
$ ./src/kvrocks -c kvrocks.conf
```

### Running test cases

***NOTE: You should install the googletest first***

```shell
make test
```

### Supported platforms

* centos 6/7
* ubuntu
* macosx

## Try kvrocks using Docker

```
$ docker run -it -p 6666:6666 kvrocks/kvrocks
$ redis-cli -p 6666

127.0.0.1:6666> get a
(nil)
```

##  Namespace

namespace was used to isolate data between users. unlike all the redis databases can be visited by `requirepass`, we use one token per namespace. `requirepass` was regraded as admin token, only admin token allows to access the namespace command, as well as some commands like `config`, `slaveof`, `bgsave`, etc…

```
# add token
127.0.0.1:6666>  namespace add ns1 mytoken
OK

# update token
127.0.0.1:6666> namespace set ns1 new_token
OK

# list namespace
127.0.0.1:6666> namespace get *
1) "ns1"
2) "new_token"
3) "__namespace"
4) "foobared"

# delete namespace
127.0.0.1:6666> namespace del ns1
OK
```

## DOCs

* [supported commands](https://github.com/kvrockslabs/kvrocks/blob/master/docs/support-commands.md)
* [design complex kv on rocksdb](https://github.com/kvrockslabs/kvrocks/blob/master/docs/metadata-design.md)
* [replication design](https://github.com/kvrockslabs/kvrocks/blob/master/docs/replication-design.md)

## Migrate tools

* migrate from redis to kvrocks, use [redis-migrate-tool](https://github.com/vipshop/redis-migrate-tool) which was developed by vipshop
* migrate from kvrocks to redis. use `kvrocks2redis` in build dir

## Performance

#### Hardware

* CPU: 48 cores Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz
* Memory: 32 GiB
* NET:  Intel Corporation I350 Gigabit Network Connection
* DISK: 2TB NVMe Intel SSD DC P460

>  Benchmark Client:  multi-thread redis-benchmark(unstable branch)

 #### 1. Commands QPS

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/kvrockslabs/kvrocks/master/docs/images/chart-commands.png)

#### 2.  QPS on different payloads

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/kvrockslabs/kvrocks/master/docs/images/chart-values.png)

#### 3. QPS on different workers

> kvrocks: workers = 16, benchmark: 8 threads/ 512 conns / 128 payload

latency: 99.9% < 10ms

![image](https://raw.githubusercontent.com/kvrockslabs/kvrocks/master/docs/images/chart-threads.png)

## License

Kvrocks is under the BSD-3-Clause license. See the LICENSE file for details.

## 微信公众号

<img src="docs/images/wechat_account.jpg" alt="微信公众号" />
