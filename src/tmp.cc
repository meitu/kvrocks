CommandAttributes redisCommandTable[] = {
    ADD_CMD("auth", 2, "read-only", 0, 0, 0, CommandAuth),
    ADD_CMD("ping", 1, "read-only", 0, 0, 0, CommandPing),
    ADD_CMD("select", 2, "read-only", 0, 0, 0, CommandSelect),
    ADD_CMD("info", -1, "read-only", 0, 0, 0, CommandInfo),
    ADD_CMD("role", 1, "read-only", 0, 0, 0, CommandRole),
    ADD_CMD("config", -2, "read-only", 0, 0, 0, CommandConfig),
    ADD_CMD("namespace", -3, "read-only", 0, 0, 0, CommandNamespace),
    ADD_CMD("keys", 2, "read-only", 0, 0, 0, CommandKeys),
    ADD_CMD("flushdb", 1, "read-only", 0, 0, 0, CommandFlushDB),
    ADD_CMD("flushall", 1, "read-only", 0, 0, 0, CommandFlushAll),
    ADD_CMD("dbsize", -1, "read-only", 0, 0, 0, CommandDBSize),
    ADD_CMD("slowlog", -2, "read-only", 0, 0, 0, CommandSlowlog),
    ADD_CMD("perflog", -2, "read-only", 0, 0, 0, CommandPerfLog),
    ADD_CMD("client", -2, "read-only", 0, 0, 0, CommandClient),
    ADD_CMD("monitor", 1, "read-only", 0, 0, 0, CommandMonitor),
    ADD_CMD("shutdown", 1, "read-only", 0, 0, 0, CommandShutdown),
    ADD_CMD("quit", 1, "read-only", 0, 0, 0, CommandQuit),
    ADD_CMD("scan", -2, "read-only", 0, 0, 0, CommandScan),
    ADD_CMD("randomkey", 1, "read-only", 0, 0, 0, CommandRandomKey),
    ADD_CMD("debug", -2, "read-only", 0, 0, 0, CommandDebug),
    ADD_CMD("command", -1, "read-only", 0, 0, 0, CommandCommand),

    ADD_CMD("ttl", 2, "read-only", 1, 1, 1, CommandTTL),
    ADD_CMD("pttl", 2, "read-only", 1, 1, 1, CommandPTTL),
    ADD_CMD("type", 2, "read-only", 1, 1, 1, CommandType),
    ADD_CMD("object", 3, "read-only", 2, 2, 1, CommandObject),
    ADD_CMD("exists", -2, "read-only", 1, -1, 1, CommandExists),
    ADD_CMD("persist", 2, "write", 1, 1, 1, CommandPersist),
    ADD_CMD("expire", 3, "write", 1, 1, 1, CommandExpire),
    ADD_CMD("pexpire", 3, "write", 1, 1, 1, CommandPExpire),
    ADD_CMD("expireat", 3, "write", 1, 1, 1, CommandExpireAt),
    ADD_CMD("pexpireat", 3, "write", 1, 1, 1, CommandPExpireAt),
    ADD_CMD("del", -2, "write", 1, -1, 1, CommandDel),

    ADD_CMD("get", 2, "read-only", 1, 1, 1, CommandGet),
    ADD_CMD("strlen", 2, "read-only", 1, 1, 1, CommandStrlen),
    ADD_CMD("getset", 3, "write", 1, 1, 1, CommandGetSet),
    ADD_CMD("getrange", 4, "read-only", 1, 1, 1, CommandGetRange),
    ADD_CMD("setrange", 4, "write", 1, 1, 1, CommandSetRange),
    ADD_CMD("mget", -2, "read-only", 1, -1, 1, CommandMGet),
    ADD_CMD("append", 3, "write", 1, 1, 1, CommandAppend),
    ADD_CMD("set", -3, "write", 1, 1, 1, CommandSet),
    ADD_CMD("setex", 4, "write", 1, 1, 1, CommandSetEX),
    ADD_CMD("psetex", 4, "write", 1, 1, 1, CommandPSetEX),
    ADD_CMD("setnx", 3, "write", 1, 1, 1, CommandSetNX),
    ADD_CMD("msetnx", -3, "write", 1, -1, 2, CommandMSetNX),
    ADD_CMD("mset", -3, "write", 1, -1, 2, CommandMSet),
    ADD_CMD("incrby", 3, "write", 1, 1, 1, CommandIncrBy),
    ADD_CMD("incrbyfloat", 3, "write", 1, 1, 1, CommandIncrByFloat),
    ADD_CMD("incr", 2, "write", 1, 1, 1, CommandIncr),
    ADD_CMD("decrby", 3, "write", 1, 1, 1, CommandDecrBy),
    ADD_CMD("decr", 2, "write", 1, 1, 1, CommandDecr),

    ADD_CMD("getbit", 3, "read-only", 1, 1, 1, CommandGetBit),
    ADD_CMD("setbit", 4, "write", 1, 1, 1, CommandSetBit),
    ADD_CMD("bitcount", -2, "read-only", 1, 1, 1, CommandBitCount),
    ADD_CMD("bitpos", -3, "read-only", 1, 1, 1, CommandBitPos),

    ADD_CMD("hget", 3, "read-only", 1, 1, 1, CommandHGet),
    ADD_CMD("hincrby", 4, "write", 1, 1, 1, CommandHIncrBy),
    ADD_CMD("hincrbyfloat", 4, "write", 1, 1, 1, CommandHIncrByFloat),
    ADD_CMD("hset", 4, "write", 1, 1, 1, CommandHSet),
    ADD_CMD("hsetnx", 4, "write", 1, 1, 1, CommandHSetNX),
    ADD_CMD("hdel", -3, "write", 1, 1, 1, CommandHDel),
    ADD_CMD("hstrlen", 3, "read-only", 1, 1, 1, CommandHStrlen),
    ADD_CMD("hexists", 3, "read-only", 1, 1, 1, CommandHExists),
    ADD_CMD("hlen", 2, "read-only", 1, 1, 1, CommandHLen),
    ADD_CMD("hmget", -3, "read-only", 1, 1, 1, CommandHMGet),
    ADD_CMD("hmset", -4, "write", 1, 1, 1, CommandHMSet),
    ADD_CMD("hkeys", 2, "read-only", 1, 1, 1, CommandHKeys),
    ADD_CMD("hvals", 2, "read-only", 1, 1, 1, CommandHVals),
    ADD_CMD("hgetall", 2, "read-only", 1, 1, 1, CommandHGetAll),
    ADD_CMD("hscan", -3, "read-only", 1, 1, 1, CommandHScan),

    ADD_CMD("lpush", -3, "write", 1, 1, 1, CommandLPush),
    ADD_CMD("rpush", -3, "write", 1, 1, 1, CommandRPush),
    ADD_CMD("lpushx", -3, "write", 1, 1, 1, CommandLPushX),
    ADD_CMD("rpushx", -3, "write", 1, 1, 1, CommandRPushX),
    ADD_CMD("lpop", 2, "write", 1, 1, 1, CommandLPop),
    ADD_CMD("rpop", 2, "write", 1, 1, 1, CommandRPop),
    ADD_CMD("blpop", -3, "write", 1, -2, 1, CommandBLPop),
    ADD_CMD("brpop", -3, "write", 1, -2, 1, CommandBRPop),
    ADD_CMD("lrem", 4, "read-only", 1, 1, 1, CommandLRem),
    ADD_CMD("linsert", 5, "read-only", 1, 1, 1, CommandLInsert),
    ADD_CMD("lrange", 4, "read-only", 1, 1, 1, CommandLRange),
    ADD_CMD("lindex", 3, "read-only", 1, 1, 1, CommandLIndex),
    ADD_CMD("ltrim", 4, "write", 1, 1, 1, CommandLTrim),
    ADD_CMD("llen", 2, "read-only", 1, 1, 1, CommandLLen),
    ADD_CMD("lset", 4, "write", 1, 1, 1, CommandLSet),
    ADD_CMD("rpoplpush", 3, "write", 1, 2, 1, CommandRPopLPUSH),

    ADD_CMD("sadd", -3, "write", 1, 1, 1, CommandSAdd),
    ADD_CMD("srem", -3, "write", 1, 1, 1, CommandSRem),
    ADD_CMD("scard", 2, "read-only", 1, 1, 1, CommandSCard),
    ADD_CMD("smembers", 2, "read-only", 1, 1, 1, CommandSMembers),
    ADD_CMD("sismember", 3, "read-only", 1, 1, 1, CommandSIsMember),
    ADD_CMD("spop", -2, "write", 1, 1, 1, CommandSPop),
    ADD_CMD("srandmember", -2, "read-only", 1, 1, 1, CommandSRandMember),
    ADD_CMD("smove", 4, "write", 1, 2, 1, CommandSMove),
    ADD_CMD("sdiff", -2, "read-only", 1, -1, 1, CommandSDiff),
    ADD_CMD("sunion", -2, "read-only", 1, -1, 1, CommandSUnion),
    ADD_CMD("sinter", -2, "read-only", 1, -1, 1, CommandSInter),
    ADD_CMD("sdiffstore", -3, "read-only", 1, -1, 1, CommandSDiffStore),
    ADD_CMD("sunionstore", -3, "read-only", 1, -1, 1, CommandSUnionStore),
    ADD_CMD("sinterstore", -3, "read-only", 1, -1, 1, CommandSInterStore),
    ADD_CMD("sscan", -3, "read-only", 1, 1, 1, CommandSScan),

    ADD_CMD("zadd", -4, "write", 1, 1, 1, CommandZAdd),
    ADD_CMD("zcard", 2, "read-only", 1, 1, 1, CommandZCard),
    ADD_CMD("zcount", 4, "read-only", 1, 1, 1, CommandZCount),
    ADD_CMD("zincrby", 4, "write", 1, 1, 1, CommandZIncrBy),
    ADD_CMD("zinterstore", -4, "write", 1, 1, 1, CommandZInterStore),
    ADD_CMD("zlexcount", 4, "write", 1, 1, 1, CommandZLexCount),
    ADD_CMD("zpopmax", -2, "write", 1, 1, 1, CommandZPopMax),
    ADD_CMD("zpopmin", -2, "write", 1, 1, 1, CommandZPopMin),
    ADD_CMD("zrange", -4, "read-only", 1, 1, 1, CommandZRange),
    ADD_CMD("zrevrange", -4, "read-only", 1, 1, 1, CommandZRevRange),
    ADD_CMD("zrangebylex", -4, "read-only", 1, 1, 1, CommandZRangeByLex),
    ADD_CMD("zrangebyscore", -4, "read-only", 1, 1, 1, CommandZRangeByScore),
    ADD_CMD("zrank", 3, "read-only", 1, 1, 1, CommandZRank),
    ADD_CMD("zrem", -3, "write", 1, 1, 1, CommandZRem),
    ADD_CMD("zremrangebyrank", 4, "write", 1, 1, 1, CommandZRemRangeByRank),
    ADD_CMD("zremrangebyscore", -4, "write", 1, 1, 1, CommandZRemRangeByScore),
    ADD_CMD("zremrangebylex", 4, "write", 1, 1, 1, CommandZRemRangeByLex),
    ADD_CMD("zrevrangebyscore", -4, "read-only", 1, 1, 1, CommandZRevRangeByScore),
    ADD_CMD("zrevrank", 3, "read-only", 1, 1, 1, CommandZRevRank),
    ADD_CMD("zscore", 3, "read-only", 1, 1, 1, CommandZScore),
    ADD_CMD("zmscore", -3, "read-only", 1, 1, 1, CommandZMScore),
    ADD_CMD("zscan", -3, "read-only", 1, 1, 1, CommandZScan),
    ADD_CMD("zunionstore", -4, "write", 1, 1, 1, CommandZUnionStore),

    ADD_CMD("geoadd", -5, "write", 1, 1, 1, CommandGeoAdd),
    ADD_CMD("geodist", -4, "read-only", 1, 1, 1, CommandGeoDist),
    ADD_CMD("geohash", -3, "read-only", 1, 1, 1, CommandGeoHash),
    ADD_CMD("geopos", -3, "read-only", 1, 1, 1, CommandGeoPos),
    ADD_CMD("georadius", -6, "write", 1, 1, 1, CommandGeoRadius),
    ADD_CMD("georadiusbymember", -5, "write", 1, 1, 1, CommandGeoRadiusByMember),
    ADD_CMD("georadius_ro", -6, "read-only", 1, 1, 1, CommandGeoRadiusReadonly),
    ADD_CMD("georadiusbymember_ro", -5, "read-only", 1, 1, 1, CommandGeoRadiusByMemberReadonly),

    ADD_CMD("publish", 3, "read-only", 0, 0, 0, CommandPublish),
    ADD_CMD("subscribe", -2, "read-only", 0, 0, 0, CommandSubscribe),
    ADD_CMD("unsubscribe", -1, "read-only", 0, 0, 0, CommandUnSubscribe),
    ADD_CMD("psubscribe", -2, "read-only", 0, 0, 0, CommandPSubscribe),
    ADD_CMD("punsubscribe", -1, "read-only", 0, 0, 0, CommandPUnSubscribe),
    ADD_CMD("pubsub", -2, "read-only", 0, 0, 0, CommandPubSub),

    ADD_CMD("siadd", -3, "write", 1, 1, 1, CommandSortedintAdd),
    ADD_CMD("sirem", -3, "write", 1, 1, 1, CommandSortedintRem),
    ADD_CMD("sicard", 2, "read-only", 1, 1, 1, CommandSortedintCard),
    ADD_CMD("siexists", -3, "read-only", 1, 1, 1, CommandSortedintExists),
    ADD_CMD("sirange", -4, "read-only", 1, 1, 1, CommandSortedintRange),
    ADD_CMD("sirevrange", -4, "read-only", 1, 1, 1, CommandSortedintRevRange),
    ADD_CMD("sirangebyvalue", -4, "read-only", 1, 1, 1, CommandSortedintRangeByValue),
    ADD_CMD("sirevrangebyvalue", -4, "read-only", 1, 1, 1, CommandSortedintRevRangeByValue),

    ADD_CMD("compact", 1, "read-only", 0, 0, 0, CommandCompact),
    ADD_CMD("bgsave", 1, "read-only", 0, 0, 0, CommandBGSave),
    ADD_CMD("flushbackup", 1, "read-only", 0, 0, 0, CommandFlushBackup),
    ADD_CMD("slaveof", 3, "read-only", 0, 0, 0, CommandSlaveOf),
    ADD_CMD("stats", 1, "read-only", 0, 0, 0, CommandStats),

    ADD_CMD("replconf", -3, "read-only", 0, 0, 0, CommandReplConf),
    ADD_CMD("psync", 2, "read-only", 0, 0, 0, CommandPSync),
    ADD_CMD("_fetch_meta", 1, "read-only", 0, 0, 0, CommandFetchMeta),
    ADD_CMD("_fetch_file", 2, "read-only", 0, 0, 0, CommandFetchFile),
    ADD_CMD("_db_name", 1, "read-only", 0, 0, 0, CommandDBName),
};