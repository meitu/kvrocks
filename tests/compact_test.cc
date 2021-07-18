#include <gtest/gtest.h>

#include "config.h"
#include "storage.h"
#include "redis_metadata.h"
#include "redis_hash.h"
#include "redis_zset.h"

TEST(Compact, Filter) {
  Config config;
  config.db_dir = "compactdb";
  config.backup_dir = "compactdb/backup";
  config.slot_id_encoded = false;

  auto storage_ = new Engine::Storage(&config);
  Status s = storage_->Open();
  assert(s.IsOK());

  int ret;
  std::string ns = "test_compact";
  auto hash = new Redis::Hash(storage_, ns);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";
  hash->Set(expired_hash_key, "f1", "v1", &ret);
  hash->Set(expired_hash_key, "f2", "v2", &ret);
  hash->Expire(expired_hash_key, 1); // expired
  usleep(10000);
  hash->Set(live_hash_key, "f1", "v1", &ret);
  hash->Set(live_hash_key, "f2", "v2", &ret);
  auto status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());

  rocksdb::DB *db = storage_->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = db->GetSnapshot();
  read_options.fill_cache = false;
  auto iter = db->NewIterator(read_options, storage_->GetCFHandle("metadata"));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string user_key, user_ns;
    ExtractNamespaceKey(iter->key(), &user_ns, &user_key, storage_->IsSlotIdEncoded());
    EXPECT_EQ(user_key, live_hash_key);
  }
  delete iter;

  iter = db->NewIterator(read_options, storage_->GetCFHandle("subkey"));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }
  delete iter;
  delete hash;

  auto zset = new Redis::ZSet(storage_, ns);
  std::string expired_zset_key = "expire_zset_key";
  std::vector<MemberScore> member_scores =  {MemberScore{"z1", 1.1}, MemberScore{"z2", 0.4}};
  zset->Add(expired_zset_key, 0, &member_scores, &ret);
  zset->Expire(expired_zset_key, 1); // expired
  usleep(10000);

  status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());

  iter = db->NewIterator(read_options, storage_->GetCFHandle("default"));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }
  delete iter;

  iter = db->NewIterator(read_options, storage_->GetCFHandle("zset_score"));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(false);  // never reach here
  }
  delete iter;

  delete zset;
}
