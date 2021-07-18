#include "config.h"
#include "server.h"
#include <map>
#include <vector>
#include <gtest/gtest.h>

TEST(Config, GetAndSet) {
  const char *path = "test.conf";
  Config config;

  config.Load(path);
  std::map<std::string, std::string> mutable_cases = {
      {"timeout" , "1000"},
      {"maxclients" , "2000"},
      {"max-backup-to-keep" , "1"},
      {"max-backup-keep-hours" , "4000"},
      {"requirepass" , "mytest_requirepass"},
      {"masterauth" , "mytest_masterauth"},
      {"compact-cron" , "1 2 3 4 5"},
      {"bgsave-cron" , "5 4 3 2 1"},
      {"max-io-mb" , "5000"},
      {"max-db-size" , "6000"},
      {"max-replication-mb" , "7000"},
      {"slave-serve-stale-data" , "no"},
      {"slave-read-only" , "no"},
      {"slave-priority" , "101"},
      {"slowlog-log-slower-than" , "1234"},
      {"slowlog-max-len" , "123"},
      {"profiling-sample-ratio" , "50"},
      {"profiling-sample-record-max-len" , "1"},
      {"profiling-sample-record-threshold-ms" , "50"},
      {"profiling-sample-commands" , "get,set"},

      {"rocksdb.compression" , "no"},
      {"rocksdb.max_open_files" , "1234"},
      {"rocksdb.write_buffer_size" , "1234"},
      {"rocksdb.max_write_buffer_number" , "1"},
      {"rocksdb.max_background_compactions" , "2"},
      {"rocksdb.max_sub_compactions" , "3"},
      {"rocksdb.delayed_write_rate" , "1234"},
      {"rocksdb.stats_dump_period_sec" , "600"},
      {"rocksdb.compaction_readahead_size" , "1024"},
      {"rocksdb.level0_slowdown_writes_trigger" , "50"},
      {"rocksdb.level0_stop_writes_trigger", "100"},
  };
  std::vector<std::string> values;
  for (const auto &iter : mutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK());
    config.Get(iter.first, &values);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    EXPECT_EQ(values[1], iter.second);
  }
  ASSERT_TRUE(config.Rewrite().IsOK());
  config.Load(path);
  for (const auto &iter : mutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK());
    config.Get(iter.first, &values);
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    EXPECT_EQ(values[1], iter.second);
  }
  unlink(path);

  std::map<std::string, std::string> immutable_cases = {
      {"daemonize", "yes"},
      {"bind", "0.0.0.0"},
      {"repl-bind", "0.0.0.0"},
      {"workers", "8"},
      {"repl-workers", "8"},
      {"tcp-backlog", "500"},
      {"slaveof", "no one"},
      {"db-name", "test_dbname"},
      {"dir", "test_dir"},
      {"backup-dir", "test_dir/backup"},
      {"pidfile", "test.pid"},
      {"supervised", "no"},
      {"rocksdb.block_size", "1234"},
      {"rocksdb.target_file_size_base", "100"},
      {"rocksdb.max_background_flushes", "16"},
      {"rocksdb.wal_ttl_seconds", "10000"},
      {"rocksdb.wal_size_limit_mb", "16"},
      {"rocksdb.enable_pipelined_write", "no"},
      {"rocksdb.cache_index_and_filter_blocks", "no"},
      {"rocksdb.metadata_block_cache_size", "100"},
      {"rocksdb.subkey_block_cache_size", "100"},
  };
  for (const auto &iter : immutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_FALSE(s.IsOK());
  }
}

TEST(Namespace, Add) {
  Config config;
  config.slot_id_encoded = false;
  EXPECT_TRUE(!config.AddNamespace("ns", "t0").IsOK());
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    auto s = config.AddNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the token has already exists");
  }
  auto s = config.AddNamespace("n1", "t0");
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), "the namespace has already exists");
}

TEST(Namespace, Set) {
  Config config;
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  std::vector<std::string> new_tokens = {"nt1", "nt2'", "nt3", "nt4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    auto s = config.SetNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the namespace was not found");
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.SetNamespace(namespaces[i], new_tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, new_tokens[i]);
  }
}

TEST(Namespace, Delete) {
  Config config;
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for (const auto &ns : namespaces) {
    config.DelNamespace(ns);
    std::string token;
    config.GetNamespace(ns, &token);
    EXPECT_TRUE(token.empty());
  }
}

TEST(Namespace, RewriteNamespaces) {
  const char *path = "test.conf";
  unlink(path);
  Config config;
  config.requirepass = "test";
  config.backup_dir = "test";
  config.Load(path) ;
  config.slot_id_encoded = false;
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  auto s = config.Rewrite();
  std::cout << s.Msg() << std::endl;
  EXPECT_TRUE(s.IsOK());
  Config new_config;
  s = new_config.Load(path) ;
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    new_config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  unlink(path);
}
