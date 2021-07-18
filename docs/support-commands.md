# Support Commands

## String Commands

| Command     | Supported OR Not | Desc                 |
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
| psetex      | √                | only supports second |
| set         | √                |                      |
| setex       | √                |                      |
| setnx       | √                |                      |
| setrange    | √                |                      |
| strlen      | √                |                      |

## Hash Commands

| Command      | Supported OR Not | Desc |
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

## List Commands

| Command    | Supported OR Not | Desc                                                         |
| ---------- | ---------------- | ------------------------------------------------------------ |
| blpop      | √                |                                                              |
| brpop      | √                |                                                              |
| brpoplpush | X                |                                                              |
| lindex     | √                | Caution: linsert is O(N) operation, don't use it when list was extreme long |
| linsert    | √                |                                                              |
| llen       | √                |                                                              |
| lpop       | √                |                                                              |
| lpush      | √                |                                                              |
| lpushx     | √                |                                                              |
| lrange     | √                |                                                              |
| lrem       | √                | Caution: lrem is O(N) operation, don't use it when list was extreme long |
| lset       | √                |                                                              |
| ltrim      | √                | Caution: ltrim is O(N) operation, don't use it when list was extreme long |
| rpop       | √                |                                                              |
| rpoplpush  | √                |                                                              |
| rpush      | √                |                                                              |
| rpushx     | √                |                                                              |



## Set Commands

| Command     | Supported OR Not | Desc                                  |
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
| spop        | √                | pop the member with key oreder        |
| srandmember | √                | always first N members if not changed |
| srem        | √                |                                       |
| sunion      | √                |                                       |
| sunionstore | √                |                                       |
| sscan       | √                |                                       |

## ZSet Commands

| Command          | Supported OR Not | Desc        |
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
| zmscore          | √                |multi zscore |
| zunionstore      | √                |             |

## Key Commands

| Command   | Supported OR Not | Desc                 |
| --------- | ---------------- | -------------------- |
| del       | √                |                      |
| dump      | X                |                      |
| exists    | √                |                      |
| expire    | √                |                      |
| expireat  | √                |                      |
| keys      | √                |                      |
| persist   | √                |                      |
| pexpire   | √                | precision is seconds |
| pexpireat | √                | precision is seconds |
| pttl      | √                |                      |
| ttl       | √                |                      |
| type      | √                |                      |
| scan      | √                |                      |
| rename    | X                |                      |
| randomkey | √                |                      |

## Bit Commands

| Command  | Supported OR Not | Desc |
| -------- | ---------------- | ---- |
| getbit   | √                |      |
| setbit   | √                |      |
| bitcount | √                |      |
| bitpos   | √                |      |
| bitfield | X                |      |
| bitop    | X                |      |

**NOTE : String and Bitmap is different type in kvrocks, so you can't do bit with string, vice versa.**



## Pub/Sub Commands

| Command      | Supported OR Not | Desc |
| ------------ | ---------------- | ---- |
| psubscribe   | √                |      |
| publish      | √                |      |
| pubsub       | √                |      |
| punsubscribe | √                |      |
| subscribe    | √                |      |
| unsubscribe  | √                |      |

## Transaction Commands

| Command   | Supported OR Not | Desc |
| --------- | ---------------- | ---- |
| multi     | √                |      |
| exec      | √                |      |
| discard   | √                |      |
| watch     | X                |      |
| unwatch   | X                |      |

## Sortedint Commands

| Command            | Supported OR Not | Desc                                               |
| -----------------  | ---------------- | -------------------------------------------------  |
| sicard             | √                | like scard                                         |
| siadd              | √                | like sadd, but member is int                       |
| sirem              | √                | like srem, but member is int                       |
| sirange            | √                | sirange key offset count cursor since_id           |
| sirevrange         | √                | sirevrange key offset count cursor max_id          |
| siexists           | √                | siexists key member1 (member2 ...)                 |
| sirangebyvalue     | √                | sirangebyvalue key min max (LIMIT offset count)    |
| sirevrangebyvalue  | √                | sirevrangebyvalue key max min (LIMIT offset count) |

## Cluster Subcommands

| Subcommand   | Supported OR Not | Desc |
| ------------ | ---------------- | ---- |
| info         | √                |      |
| nodes        | √                |      |
| slots        | √                |      |
| keyslot      | √                |      |

## Server Commands

| Command      | Supported OR Not | Desc |
| ------------ | ---------------- | ---- |
| monitor      | √                |      |
| info         | √                |      |
| role         | √                |      |
| config       | √                |      |
| dbsize       | √                |      |
| namespace    | √                |      |
| flushdb      | √                |      |
| flushall     | √                |      |
| command      | √                |      |

**NOTE : The db size was updated async after execute `dbsize scan` command**

## GEO Commands

| Command           | Supported OR Not | Desc |
| ------------      | ---------------- | ---- |
| geoadd            | √                |      |
| geodist           | √                |      |
| geohash           | √                |      |
| geopos            | √                |      |
| georadius         | √                |      |
| georadiusbymember | √                |      |

## Hyperloglog Commands

**Not Supported**
