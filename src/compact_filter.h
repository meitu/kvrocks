#pragma once

#include <vector>
#include <memory>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/compaction_filter.h>

#include "redis_metadata.h"
#include "storage.h"

namespace Engine {
class MetadataFilter : public rocksdb::CompactionFilter {
 public:
  explicit MetadataFilter(Storage *storage): stor_(storage) {}
  const char *Name() const override { return "MetadataFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;
 private:
  Engine::Storage *stor_;
};

class MetadataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit MetadataFilterFactory(Engine::Storage *storage) {
    stor_ = storage;
  }
  const char *Name() const override { return "MetadataFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new MetadataFilter(stor_));
  }

 private:
  Engine::Storage *stor_ = nullptr;
};

class SubKeyFilter : public rocksdb::CompactionFilter {
 public:
  explicit SubKeyFilter(Storage *storage)
      : cached_key_(""),
        cached_metadata_(""),
        stor_(storage) {}

  const char *Name() const override { return "SubkeyFilter"; }
  bool IsKeyExpired(const InternalKey &ikey, const Slice &value) const;
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;

 protected:
  mutable std::string cached_key_;
  mutable std::string cached_metadata_;
  Engine::Storage *stor_;
};

class SubKeyFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit SubKeyFilterFactory(Engine::Storage *storage) {
    stor_ = storage;
  }

  const char *Name() const override { return "SubKeyFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new SubKeyFilter(stor_));
  }

 private:
  Engine::Storage *stor_ = nullptr;
};

class PubSubFilter : public rocksdb::CompactionFilter {
 public:
  const char *Name() const override { return "PubSubFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override { return true; }
};

class PubSubFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  PubSubFilterFactory() = default;
  const char *Name() const override { return "PubSubFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new PubSubFilter());
  }
};
}  // namespace Engine
