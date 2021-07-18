#pragma once

#include <string>
#include <vector>
#include <atomic>

#include <rocksdb/status.h>

#include "encoding.h"

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet,
  kRedisBitmap,
  kRedisSortedint,
};

enum RedisCommand {
  kRedisCmdLSet,
  kRedisCmdLInsert,
  kRedisCmdLTrim,
  kRedisCmdLPop,
  kRedisCmdRPop,
  kRedisCmdLRem,
  kRedisCmdLPush,
  kRedisCmdRPush,
  kRedisCmdExpire,
};

const std::vector<std::string> RedisTypeNames = {
    "none", "string", "hash",
    "list", "set", "zset", "bitmap", "sortedint"
};

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key, bool slot_id_encoded);
void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key, bool slot_id_encoded);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded);
  explicit InternalKey(Slice input, bool slot_id_encoded);
  ~InternalKey();

  Slice GetNamespace() const;
  Slice GetKey() const;
  Slice GetSubKey() const;
  uint64_t GetVersion() const;
  void Encode(std::string *out);
  bool operator==(const InternalKey &that) const;

 private:
  Slice namespace_;
  Slice key_;
  Slice sub_key_;
  uint64_t version_;
  char *buf_;
  char prealloc_[256];
  uint16_t slotid_;
  bool slot_id_encoded_;
};

class Metadata {
 public:
  uint8_t flags;
  int expire;
  uint64_t version;
  uint32_t size;

 public:
  explicit Metadata(RedisType type, bool generate_version = true);
  static void InitVersionCounter();

  RedisType Type() const;
  virtual int32_t TTL() const;
  virtual timeval Time() const;
  virtual bool Expired() const;
  virtual void Encode(std::string *dst);
  virtual rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const Metadata &that) const;

 private:
  uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  explicit HashMetadata(bool generate_version = true) : Metadata(kRedisHash, generate_version){}
};

class SetMetadata : public Metadata {
 public:
  explicit SetMetadata(bool generate_version = true): Metadata(kRedisSet, generate_version) {}
};

class ZSetMetadata : public Metadata {
 public:
  explicit ZSetMetadata(bool generate_version = true): Metadata(kRedisZSet, generate_version){}
};

class BitmapMetadata : public Metadata {
 public:
  explicit BitmapMetadata(bool generate_version = true): Metadata(kRedisBitmap, generate_version){}
};

class SortedintMetadata : public Metadata {
 public:
  explicit SortedintMetadata(bool generate_version = true) : Metadata(kRedisSortedint, generate_version) {}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  explicit ListMetadata(bool generate_version = true);
 public:
  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};
