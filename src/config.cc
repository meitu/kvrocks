#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <limits>
#include <glog/logging.h>
#include <rocksdb/env.h>

#include "config.h"
#include "util.h"
#include "status.h"
#include "cron.h"
#include "server.h"
#include "log_collector.h"
#include "semisync_master.h"

const char *kDefaultNamespace = "__namespace";
configEnum compression_type_enum[] = {
    {"no", rocksdb::CompressionType::kNoCompression},
    {"snappy", rocksdb::CompressionType::kSnappyCompression},
    {nullptr, 0}
};
configEnum supervised_mode_enum[] = {
    {"no", SUPERVISED_NONE},
    {"auto", SUPERVISED_AUTODETECT},
    {"upstart", SUPERVISED_UPSTART},
    {"systemd", SUPERVISED_SYSTEMD},
    {nullptr, 0}
};

ConfigField::~ConfigField() = default;

std::string trimRocksDBPrefix(std::string s) {
  if (strncasecmp(s.data(), "rocksdb.", 8)) return s;
  return s.substr(8, s.size()-8);
}

int configEnumGetValue(configEnum *ce, const char *name) {
  while (ce->name != nullptr) {
    if (!strcasecmp(ce->name, name)) return ce->val;
    ce++;
  }
  return INT_MIN;
}

const char *configEnumGetName(configEnum *ce, int val) {
  while (ce->name != nullptr) {
    if (ce->val == val) return ce->name;
    ce++;
  }
  return nullptr;
}

Config::Config() {
  struct FieldWrapper {
    const char *name;
    bool readonly;
    ConfigField *field;
  };
  std::vector<FieldWrapper> fields = {
      {"daemonize", true, new YesNoField(&daemonize, false)},
      {"bind", true, new StringField(&binds_, "127.0.0.1")},
      {"port", true, new IntField(&port, 6666, 1, 65535)},
      {"workers", true, new IntField(&workers, 8, 1, 256)},
      {"repl-workers", true, new IntField(&repl_workers, 4, 1, 256)},
      {"timeout", false, new IntField(&timeout, 0, 0, INT_MAX)},
      {"tcp-backlog", true, new IntField(&backlog, 511, 0, INT_MAX)},
      {"maxclients", false, new IntField(&maxclients, 10240, 0, INT_MAX)},
      {"max-backup-to-keep", false, new IntField(&max_backup_to_keep, 1, 0, 1)},
      {"max-backup-keep-hours", false, new IntField(&max_backup_keep_hours, 0, 0, INT_MAX)},
      {"master-use-repl-port", false, new YesNoField(&master_use_repl_port, false)},
      {"requirepass", false, new StringField(&requirepass, "")},
      {"masterauth", false, new StringField(&masterauth, "")},
      {"slaveof", true, new StringField(&slaveof_, "")},
      {"compact-cron", false, new StringField(&compact_cron_, "")},
      {"bgsave-cron", false, new StringField(&bgsave_cron_, "")},
      {"compaction-checker-range", false, new StringField(&compaction_checker_range_, "")},
      {"db-name", true, new StringField(&db_name, "changeme.name")},
      {"dir", true, new StringField(&dir, "/tmp/kvrocks")},
      {"backup-dir", true, new StringField(&backup_dir, "")},
      {"log-dir", true, new StringField(&log_dir, "")},
      {"pidfile", true, new StringField(&pidfile, "")},
      {"max-io-mb", false, new IntField(&max_io_mb, 500, 0, INT_MAX)},
      {"max-db-size", false, new IntField(&max_db_size, 0, 0, INT_MAX)},
      {"max-replication-mb", false, new IntField(&max_replication_mb, 0, 0, INT_MAX)},
      {"supervised", true, new EnumField(&supervised_mode, supervised_mode_enum, SUPERVISED_NONE)},
      {"slave-serve-stale-data", false, new YesNoField(&slave_serve_stale_data, true)},
      {"slave-empty-db-before-fullsync", false, new YesNoField(&slave_empty_db_before_fullsync, false)},
      {"slave-priority", false, new IntField(&slave_priority, 100, 0, INT_MAX)},
      {"slave-read-only", false, new YesNoField(&slave_readonly, true)},
      {"profiling-sample-ratio", false, new IntField(&profiling_sample_ratio, 0, 0, 100)},
      {"profiling-sample-record-max-len", false, new IntField(&profiling_sample_record_max_len, 256, 0, INT_MAX)},
      {"profiling-sample-record-threshold-ms",
       false, new IntField(&profiling_sample_record_threshold_ms, 100, 0, INT_MAX)},
      {"slowlog-log-slower-than", false, new IntField(&slowlog_log_slower_than, 200000, -1, INT_MAX)},
      {"profiling-sample-commands", false, new StringField(&profiling_sample_commands_, "")},
      {"slowlog-max-len", false, new IntField(&slowlog_max_len, 128, 0, INT_MAX)},
      {"purge-backup-on-fullsync", false, new YesNoField(&purge_backup_on_fullsync, false)},
      {"semi-sync-test", false, new YesNoField(&semi_sync_test, false)},
      {"semi-sync-enable", false, new YesNoField(&semi_sync_enable, false)},
      {"semi-sync-wait-for-slave-count", false, new IntField(&semi_sync_wait_for_slave_count, 1, 1, INT_MAX)},
      /* rocksdb options */
      {"rocksdb.compression", false, new EnumField(&RocksDB.compression, compression_type_enum, 0)},
      {"rocksdb.block_size", true, new IntField(&RocksDB.block_size, 4096, 0, INT_MAX)},
      {"rocksdb.max_open_files", false, new IntField(&RocksDB.max_open_files, 4096, -1, INT_MAX)},
      {"rocksdb.write_buffer_size", false, new IntField(&RocksDB.write_buffer_size, 64, 0, 4096)},
      {"rocksdb.max_write_buffer_number", false, new IntField(&RocksDB.max_write_buffer_number, 4, 0, 256)},
      {"rocksdb.target_file_size_base", true, new IntField(&RocksDB.target_file_size_base, 128, 1, 1024)},
      {"rocksdb.max_background_compactions", false, new IntField(&RocksDB.max_background_compactions, 2, 0, 32)},
      {"rocksdb.max_background_flushes", true, new IntField(&RocksDB.max_background_flushes, 2, 0, 32)},
      {"rocksdb.max_sub_compactions", false, new IntField(&RocksDB.max_sub_compactions, 1, 0, 16)},
      {"rocksdb.delayed_write_rate", false, new Int64Field(&RocksDB.delayed_write_rate, 0, 0, INT64_MAX)},
      {"rocksdb.wal_ttl_seconds", true, new IntField(&RocksDB.WAL_ttl_seconds, 3*3600, 0, INT_MAX)},
      {"rocksdb.wal_size_limit_mb", true, new IntField(&RocksDB.WAL_size_limit_MB, 16384, 0, INT_MAX)},
      {"rocksdb.max_total_wal_size", false, new IntField(&RocksDB.max_total_wal_size, 64*4*2, 0, INT_MAX)},
      {"rocksdb.disable_auto_compactions", false, new YesNoField(&RocksDB.disable_auto_compactions, false)},
      {"rocksdb.enable_pipelined_write", true, new YesNoField(&RocksDB.enable_pipelined_write, false)},
      {"rocksdb.stats_dump_period_sec", false, new IntField(&RocksDB.stats_dump_period_sec, 0, 0, INT_MAX)},
      {"rocksdb.cache_index_and_filter_blocks", true, new YesNoField(&RocksDB.cache_index_and_filter_blocks, false)},
      {"rocksdb.subkey_block_cache_size", true, new IntField(&RocksDB.subkey_block_cache_size, 2048, 0, INT_MAX)},
      {"rocksdb.metadata_block_cache_size", true, new IntField(&RocksDB.metadata_block_cache_size, 2048, 0, INT_MAX)},
      {"rocksdb.compaction_readahead_size", false, new IntField(&RocksDB.compaction_readahead_size, 2*MiB, 0, 64*MiB)},
      {"rocksdb.level0_slowdown_writes_trigger",
       false, new IntField(&RocksDB.level0_slowdown_writes_trigger, 20, 1, 1024)},
      {"rocksdb.level0_stop_writes_trigger",
       false, new IntField(&RocksDB.level0_stop_writes_trigger, 40, 1, 1024)},
  };
  for (const auto &wrapper : fields) {
    auto field = wrapper.field;
    field->readonly = wrapper.readonly;
    fields_.insert({wrapper.name, field});
  }
  initFieldValidator();
  initFieldCallback();
}

// The validate function would be invoked before the field was set,
// to make sure that new value is valid.
void Config::initFieldValidator() {
  std::map<std::string, validate_fn> validators = {
      {"requirepass", [this](const std::string& k, const std::string& v)->Status {
        if (v.empty() && !tokens.empty()) {
          return Status(Status::NotOK, "requirepass empty not allowed while the namespace exists");
        }
        return Status::OK();
      }},
      {"compact-cron", [this](const std::string& k, const std::string& v)->Status {
        std::vector<std::string> args;
        Util::Split(v, " \t", &args);
        return compact_cron.SetScheduleTime(args);
      }},
      {"bgsave-cron", [this](const std::string& k, const std::string& v)->Status {
        std::vector<std::string> args;
        Util::Split(v, " \t", &args);
        return bgsave_cron.SetScheduleTime(args);
      }},
      {"compaction-checker-range", [this](const std::string& k, const std::string& v)->Status {
        std::vector<std::string> args;
        if (v.empty()) {
          compaction_checker_range.Start = -1;
          compaction_checker_range.Stop = -1;
          return Status::OK();
        }
        Util::Split(v, "-", &args);
        if (args.size() != 2) {
          return Status(Status::NotOK, "invalid range format, the range should be between 0 and 24");
        }
        int64_t start, stop;
        Status s = Util::StringToNum(args[0], &start, 0, 24);
        if (!s.IsOK()) return s;
        s = Util::StringToNum(args[1], &stop, 0, 24);
        if (!s.IsOK()) return s;
        if (start > stop)  return Status(Status::NotOK, "invalid range format, start should be smaller than stop");
        compaction_checker_range.Start = start;
        compaction_checker_range.Stop = stop;
        return Status::OK();
      }},
  };
  for (const auto& iter : validators) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->validate = iter.second;
    }
  }
}

// The callback function would be invoked after the field was set,
// it may change related fileds or re-format the field. for example,
// when the 'dir' was set, the db-dir or backup-dir should be reset as well.
void Config::initFieldCallback() {
  auto set_db_option_cb = [](Server* srv,  const std::string &k, const std::string& v)->Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetDBOption(trimRocksDBPrefix(k), v);
  };
  auto set_cf_option_cb = [](Server* srv,  const std::string &k, const std::string& v)->Status {
    if (!srv) return Status::OK();  // srv is nullptr when load config from file
    return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), v);
  };
  std::map<std::string, callback_fn> callbacks = {
      {"dir", [this](Server* srv,  const std::string &k, const std::string& v)->Status {
        db_dir = dir + "/db";
        if (backup_dir.empty()) backup_dir = dir + "/backup";
        if (log_dir.empty()) log_dir = dir;
        checkpoint_dir = dir + "/checkpoint";
        sync_checkpoint_dir = dir + "/sync_checkpoint";
        backup_sync_dir = dir + "/backup_for_sync";
        return Status::OK();
      }},
      {"bind", [this](Server* srv,  const std::string &k,  const std::string& v)->Status {
        trimRocksDBPrefix(k);
        std::vector<std::string> args;
        Util::Split(v, " \t", &args);
        binds = std::move(args);
        return Status::OK();
      }},
      {"slaveof", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (v.empty()) {
          return Status::OK();
        }
        std::vector<std::string> args;
        Util::Split(v, " \t", &args);
        if (args.size() != 2) return Status(Status::NotOK, "wrong number of arguments");
        if (args[0] != "no" && args[1] != "one") {
          master_host = args[0];
          master_port = std::atoi(args[1].c_str());
          if (master_port <= 0 || master_port >= 65535) {
            return Status(Status::NotOK, "should be between 0 and 65535");
          }
        }
        return Status::OK();
      }},
      {"profiling-sample-commands", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();

        std::vector<std::string> cmds;
        Util::Split(v, ",", &cmds);
        profiling_sample_all_commands = false;
        profiling_sample_commands.clear();
        for (auto const &cmd : cmds) {
          if (cmd == "*") {
            profiling_sample_all_commands = true;
            profiling_sample_commands.clear();
          } else if (srv->IsCommandExists(cmd)) {
            profiling_sample_commands.insert(cmd);
          }
        }
        return Status::OK();
      }},
      {"slowlog-max-len", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->GetSlowLog()->SetMaxEntries(slowlog_max_len);
        return Status::OK();
      }},
      {"max-db-size", [](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->storage_->CheckDBSizeLimit();
        return Status::OK();
      }},
      {"max-io-mb", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->storage_->SetIORateLimit(static_cast<uint64_t>(max_io_mb));
        return Status::OK();
      }},
      {"profiling-sample-record-max-len", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        srv->GetPerfLog()->SetMaxEntries(profiling_sample_record_max_len);
        return Status::OK();
      }},
      {"rocksdb.write_buffer_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k),
                                                    std::to_string(RocksDB.write_buffer_size * MiB));
      }},
      {"rocksdb.disable_auto_compactions", [](Server* srv,
                                                        const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        std::string disable_auto_compactions = v == "yes" ? "true" : "false";
        return srv->storage_->SetColumnFamilyOption(trimRocksDBPrefix(k), disable_auto_compactions);
      }},
      {"rocksdb.max_total_wal_size", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        return srv->storage_->SetDBOption(trimRocksDBPrefix(k),
                                          std::to_string(RocksDB.max_total_wal_size * MiB));
      }},
      {"rocksdb.max_open_files", set_db_option_cb},
      {"rocksdb.stats_dump_period_sec", set_db_option_cb},
      {"rocksdb.delayed_write_rate", set_db_option_cb},
      {"rocksdb.max_background_compactions", set_db_option_cb},
      {"rocksdb.max_background_flushes", set_db_option_cb},
      {"rocksdb.compaction_readahead_size", set_db_option_cb},

      {"rocksdb.max_write_buffer_number", set_cf_option_cb},
      {"rocksdb.level0_slowdown_writes_trigger", set_cf_option_cb},
      {"rocksdb.level0_stop_writes_trigger", set_cf_option_cb},
      {"semi-sync-enable", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!ReplSemiSyncMaster::GetInstance().InitDone()) return Status::OK();

        int result;
        if (semi_sync_enable)
          result = ReplSemiSyncMaster::GetInstance().EnableMaster();
        else
          result = ReplSemiSyncMaster::GetInstance().DisableMaster();
        if (result != 0)
          return Status(Status::NotOK, "Failed to enable semi sync");
        else
          return Status::OK();
      }},
      {"semi-sync-wait-for-slave-count", [this](Server* srv, const std::string &k, const std::string& v)->Status {
        if (!srv) return Status::OK();
        if (!ReplSemiSyncMaster::GetInstance().InitDone()) return Status::OK();

        auto set_result = ReplSemiSyncMaster::GetInstance().SetWaitSlaveCount(semi_sync_wait_for_slave_count);
        if (set_result)
          return Status::OK();
        else
          return Status(Status::NotOK, "Failed to set the semi sync slave count");
      }},
  };
  for (const auto& iter : callbacks) {
    auto field_iter = fields_.find(iter.first);
    if (field_iter != fields_.end()) {
      field_iter->second->callback = iter.second;
    }
  }
}

Config::~Config() {
  for (const auto &iter : fields_) {
    delete iter.second;
  }
}

void Config::SetMaster(const std::string &host, int port) {
  master_host = host;
  master_port = port;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    iter->second->Set(master_host+" "+std::to_string(master_port));
  }
}

void Config::ClearMaster() {
  master_host.clear();
  master_port = 0;
  auto iter = fields_.find("slaveof");
  if (iter != fields_.end()) {
    iter->second->Set("no one");
  }
}

Status Config::parseConfigFromString(std::string input) {
  std::vector<std::string> kv;
  Util::Split2KV(input, " \t", &kv);

  // skip the comment and empty line
  if (kv.empty() || kv[0].front() == '#') return Status::OK();

  if (kv.size() != 2) return Status(Status::NotOK, "wrong number of arguments");
  if (kv[1] == "\"\"") return Status::OK();

  kv[0] = Util::ToLower(kv[0]);
  auto iter = fields_.find(kv[0]);
  if (iter != fields_.end()) {
    auto field = iter->second;
    if (field->validate) {
      auto s = field->validate(kv[0], kv[1]);
      if (!s.IsOK()) return s;
    }
    auto s = field->Set(kv[1]);
    if (!s.IsOK()) return s;
  }
  if (!strncasecmp(kv[0].data(), "namespace.", 10)) {
    tokens[kv[1]] = kv[0].substr(10, kv[0].size()-10);
  }
  return Status::OK();
}

Status Config::finish() {
  if (requirepass.empty() && !tokens.empty()) {
    return Status(Status::NotOK, "requirepass empty wasn't allowed while the namespace exists");
  }
  if (db_dir.empty()) db_dir = dir + "/db";
  if (backup_dir.empty()) backup_dir = dir + "/backup";
  if (log_dir.empty()) log_dir = dir;
  if (pidfile.empty()) pidfile = dir + "/kvrocks.pid";
  std::vector<std::string> createDirs = {dir};
  for (const auto &name : createDirs) {
    auto s = rocksdb::Env::Default()->CreateDirIfMissing(name);
    if (!s.ok()) return Status(Status::NotOK, s.ToString());
  }
  return Status::OK();
}

Status Config::Load(const std::string &path) {
  if (!path.empty()) {
    path_ = path;
    std::ifstream file(path_);
    if (!file.is_open()) return Status(Status::NotOK, strerror(errno));

    std::string line;
    int line_num = 1;
    while (!file.eof()) {
      std::getline(file, line);
      Status s = parseConfigFromString(line);
      if (!s.IsOK()) {
        file.close();
        return Status(Status::NotOK, "at line: #L" + std::to_string(line_num) + ", err: " + s.Msg());
      }
      line_num++;
    }
    file.close();
  } else {
    std::cout << "Warn: no config file specified, using the default config. "
                    "In order to specify a config file use kvrocks -c /path/to/kvrocks.conf" << std::endl;
  }
  for (const auto &iter : fields_) {
    if (iter.second->callback) {
      auto s = iter.second->callback(nullptr, iter.first, iter.second->ToString());
      if (!s.IsOK()) {
        return Status(Status::NotOK, s.Msg()+" in key '"+iter.first+"'");
      }
    }
  }
  return finish();
}

void Config::Get(std::string key, std::vector<std::string> *values) {
  values->clear();
  for (const auto &iter : fields_) {
    if (key == "*" || Util::ToLower(key) == iter.first) {
      values->emplace_back(iter.first);
      values->emplace_back(iter.second->ToString());
    }
  }
}

Status Config::Set(Server *svr, std::string key, const std::string &value) {
  key = Util::ToLower(key);
  auto iter = fields_.find(key);
  if (iter == fields_.end() || iter->second->readonly) {
    return Status(Status::NotOK, "Unsupported CONFIG parameter: "+key);
  }
  auto field = iter->second;
  if (field->validate) {
    auto s = field->validate(key, value);
    if (!s.IsOK()) return s;
  }
  auto s = field->Set(value);
  if (!s.IsOK()) return s;
  if (field->callback) {
    return field->callback(svr, key, value);
  }
  return Status::OK();
}

Status Config::Rewrite() {
  if (path_.empty()) {
    return Status(Status::NotOK, "the server is running without a config file");
  }
  std::vector<std::string> lines;
  std::map<std::string, std::string> new_config;
  for (const auto &iter : fields_) {
    new_config[iter.first] = iter.second->ToString();
  }
  for (const auto &iter : tokens) {
    new_config["namespace." + iter.second] = iter.first;
  }

  std::ifstream file(path_);
  if (file.is_open()) {
    std::string raw_line, trim_line, new_value;
    std::vector<std::string> kv;
    while (!file.eof()) {
      std::getline(file, raw_line);
      Util::Trim(raw_line, " \t\r\n", &trim_line);
      if (trim_line.empty() || trim_line.front() == '#') {
        lines.emplace_back(raw_line);
        continue;
      }
      Util::Split2KV(trim_line, " \t", &kv);
      if (kv.size() != 2) {
        lines.emplace_back(raw_line);
        continue;
      }
      auto iter = new_config.find(Util::ToLower(kv[0]));
      if (iter != new_config.end()) {
        if (!iter->second.empty()) lines.emplace_back(iter->first + " " + iter->second);
        new_config.erase(iter);
      } else {
        lines.emplace_back(raw_line);
      }
    }
  }
  file.close();

  std::ostringstream string_stream;
  for (const auto &line : lines) {
    string_stream << line << "\n";
  }
  for (const auto &remain : new_config) {
    if (remain.second.empty()) continue;
    string_stream << remain.first << " " << remain.second << "\n";
  }
  std::string tmp_path = path_+".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file.write(string_stream.str().c_str(), string_stream.str().size());
  output_file.close();
  if (rename(tmp_path.data(), path_.data()) < 0) {
    return Status(Status::NotOK, std::string("rename file encounter error: ")+strerror(errno));
  }
  return Status::OK();
}

Status Config::GetNamespace(const std::string &ns, std::string *token) {
  token->clear();
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      *token = iter.first;
      return Status::OK();
    }
  }
  return Status(Status::NotFound);
}

Status Config::SetNamespace(const std::string &ns, const std::string &token) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "forbidden to update the default namespace");
  }
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      tokens[token] = ns;
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::AddNamespace(const std::string &ns, const std::string &token) {
  if (requirepass.empty()) {
    return Status(Status::NotOK, "forbidden to add namespace when requirepass was empty");
  }
  auto s = isNamespaceLegal(ns);
  if (!s.IsOK()) return s;
  if (tokens.find(token) != tokens.end()) {
    return Status(Status::NotOK, "the token has already exists");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      return Status(Status::NotOK, "the namespace has already exists");
    }
  }
  tokens[token] = ns;
  return Status::OK();
}

Status Config::DelNamespace(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return Status(Status::NotOK, "forbidden to delete the default namespace");
  }
  for (const auto &iter : tokens) {
    if (iter.second == ns) {
      tokens.erase(iter.first);
      return Status::OK();
    }
  }
  return Status(Status::NotOK, "the namespace was not found");
}

Status Config::isNamespaceLegal(const std::string &ns) {
  if (ns.size() > UINT8_MAX) {
    return Status(Status::NotOK, std::string("size exceed limit ") + std::to_string(UINT8_MAX));
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return Status(Status::NotOK, std::string("namespace contain illegal letter"));
  }
  return Status::OK();
}
