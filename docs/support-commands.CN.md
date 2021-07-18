# 支持命令

## String 命令

| 命令         |  是否支持         | 描述                  |
| ----------- | ---------------- | -------------------- |
| get         | √                |                      |
| getrange    | √                |                      |
| getset      | √                |                      |
| incr        | √                |                      |
| incrby      | √                |                      |
| incrbyfloat | √                |                      |
| mget        | √                |                      |
| mset        | √                |                      |
| msetnx      | √                |                      |
| psetex      | √                |                      |
| set         | √                |                      |
| setex       | √                |                      |
| setnx       | √                |                      |
| setrange    | √                |                      |
| strlen      | √                |                      |
| append      | √                |                      |
| decrby      | √                |                      |
| decr        | √                |                      |

## Hash 命令

| 命令          | 是否支持          | 描述  |
| ------------ | ---------------- | ---- |
| hdel         | √                |      |
| hexists      | √                |      |
| hget         | √                |      |
| hgetall      | √                |      |
| hincrby      | √                |      |
| hincrbyfloat | √                |      |
| hkeys        | √                |      |
| hlen         | √                |      |
| hmget        | √                |      |
| hmset        | √                |      |
| hset         | √                |      |
| hsetnx       | √                |      |
| hstrlen      | √                |      |
| hvals        | √                |      |
| hscan        | √                |      |

## List 命令

| 命令        | 是否支持          | 描述                                                          |
| ---------- | ---------------- | ------------------------------------------------------------ |
| blpop      | √                |                                                              |
| brpop      | √                |                                                              |
| brpoplpush | X                |                                                              |
| lindex     | √                | 复杂度 O(N)， 当 list 很长时不建议使用                           |
| linsert    | √                |                                                              |
| llen       | √                |                                                              |
| lpop       | √                |                                                              |
| lpush      | √                |                                                              |
| lpushx     | √                |                                                              |
| lrange     | √                |                                                              |
| lrem       | √                | 复杂度 O(N)， 当 list 很长时不建议使用                           |
| lset       | √                |                                                              |
| ltrim      | √                | 复杂度 O(N)， 当 list 很长时不建议使用                           |
| rpop       | √                |                                                              |
| rpoplpush  | √                |                                                              |
| rpush      | √                |                                                              |
| rpushx     | √                |                                                              |

## Set 命令

| 命令         | 是否支持          | 描述                                  |
| ----------- | ---------------- | ------------------------------------- |
| sadd        | √                |                                       |
| scard       | √                |                                       |
| sdiff       | √                |                                       |
| sdiffstore  | √                |                                       |
| sinter      | √                |                                       |
| sinterstore | √                |                                       |
| sismember   | √                |                                       |
| smembers    | √                |                                       |
| smove       | √                |                                       |
| spop        | √                | 按照 key 的顺序 pop                     |
| srandmember | √                |                                       |
| srem        | √                |                                       |
| sunion      | √                |                                       |
| sunionstore | √                |                                       |
| sscan       | √                |                                       |

## ZSet 命令

| 命令              | 是否支持          | 描述         |
| ---------------- | ---------------- | ----------- |
| bzpopmin         | X                |             |
| bzpopmax         | X                |             |
| zadd             | √                |             |
| zcard            | √                |             |
| zcount           | √                |             |
| zincrby          | √                |             |
| zinterstore      | √                |             |
| zlexcount        | √                |             |
| zpopmin          | √                |             |
| zpopmax          | √                |             |
| zrange           | √                |             |
| zrangebylex      | √                |             |
| zrangebyscore    | √                |             |
| zrank            | √                |             |
| zrem             | √                |             |
| zremrangebylex   | √                |             |
| zremrangebyrank  | √                |             |
| zremrangebyscore | √                |             |
| zrevrange        | √                |             |
| zrevrangebylex   | X                |             |
| zrevrangebyscore | √                |             |
| zscan            | √                |             |
| zscore           | √                |             |
| zmscore          | √                | 多 zscore   |
| zunionstore      | √                |             |

## Key 命令

| 命令       | 是否支持          | 描述                  |
| --------- | ---------------- | -------------------- |
| del       | √                |                      |
| dump      | X                |                      |
| exists    | √                |                      |
| expire    | √                |                      |
| expireat  | √                |                      |
| keys      | √                |                      |
| persist   | √                |                      |
| pexpire   | √                | 秒级精度              |
| pexpireat | √                | 秒级精度              |
| pttl      | √                |                      |
| ttl       | √                |                      |
| type      | √                |                      |
| scan      | √                |                      |
| rename    | X                |                      |
| randomkey | √                |                      |
| object    | √                |                      |

## Bit 命令

| 命令      | 是否支持          | 描述  |
| -------- | ---------------- | ---- |
| getbit   | √                |      |
| setbit   | √                |      |
| bitcount | √                |      |
| bitpos   | √                |      |
| bitfield | X                |      |
| bitop    | X                |      |

**说明: 在 kvrocks 中，String 和 Bitmap 是不同的类型, 所以不能在 String 上进行Bit操作，反之亦然。**

## Pub/Sub 命令

| 命令          | 是否支持          | 描述  |
| ------------ | ---------------- | ---- |
| psubscribe   | √                |      |
| publish      | √                |      |
| pubsub       | √                |      |
| punsubscribe | √                |      |
| subscribe    | √                |      |
| unsubscribe  | √                |      |

## Transaction 命令

| 命令       | 是否支持          | 描述  |
| --------- | ---------------- | ---- |
| multi     | √                |      |
| exec      | √                |      |
| discard   | √                |      |
| watch     | X                |      |
| unwatch   | X                |      |

## Sortedint 命令

| 命令                | 是否支持          | 描述                                               |
| -----------------  | ---------------- | -------------------------------------------------  |
| sicard             | √                | 用法同 scard                                        |
| siadd              | √                | 用法同 sadd, 但是成员是整形                           |
| sirem              | √                | 用法同 srem, 但是成员是整形                           |
| sirange            | √                | sirange key offset count cursor since_id           |
| sirevrange         | √                | sirevrange key offset count cursor max_id          |
| siexists           | √                | siexists key member1 (member2 ...)                 |
| sirangebyvalue     | √                | sirangebyvalue key min max (LIMIT offset count)    |
| sirevrangebyvalue  | √                | sirevrangebyvalue key max min (LIMIT offset count) |

## Cluster Sub命令

| Sub命令       | 是否支持          | 描述 |
| ------------ | ---------------- | ---- |
| info         | √                |      |
| nodes        | √                |      |
| slots        | √                |      |
| keyslot      | √                |      |

## Server 命令

| 命令          | 是否支持          | 描述 |
| ------------ | ---------------- | ---- |
| auth         | √                |      |
| bgsave       | √                |      |
| client       | √                |      |
| command      | √                |      |
| config       | √                |      |
| dbsize       | √                | 当执行 `dbsize scan` 之后，db size 会异步更新    |
| debug        | √                |      |
| flushdb      | √                |      |
| flushall     | √                |      |
| info         | √                |      |
| keys         | √                |      |
| monitor      | √                |      |
| namespace    | √                |      |
| ping         | √                |      |
| quit         | √                |      |
| role         | √                |      |
| select       | √                |      |
| shutdown     | √                |      |
| slowlog      | √                |      |
| slaveof      | √                |      |

## GEO 命令

| 命令                   | 是否支持          | 描述  |
| --------------------- | ---------------- | ---- |
| geoadd                | √                |      |
| geodist               | √                |      |
| geohash               | √                |      |
| geopos                | √                |      |
| georadius             | √                |      |
| georadiusbymember     | √                |      |
| georadius_ro          | √                |      |
| georadiusbymember_ro  | √                |      |

## Hyperloglog 命令

**暂不支持**

## Kvrocks 命令

| 命令          | 是否支持          | 描述                    |
| ------------ | ---------------- | ---------------------- |
| compact      | √                | 手动 Compaction（异步）  |
| flushbackup  | √                | 手动清除过期备份（异步）   |
| perflog      | √                | 获取 Rocksdb 的性能报告  |
| stats        | √                | 获取 RocksDB 的统计信息  |