#pragma once

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <map>
#include <list>
#include <string>
#include <vector>
#include <memory>

#include "stats.h"
#include "storage.h"
#include "task_runner.h"
#include "replication.h"
#include "redis_metadata.h"
#include "log_collector.h"
#include "worker.h"
#include "rw_lock.h"
#include "cluster.h"

struct DBScanInfo {
  time_t last_scan_time = 0;
  KeyNumStats key_num_stats;
  bool is_scanning = false;
};

struct ConnContext {
  Worker *owner;
  int fd;
  ConnContext(Worker *w, int fd) : owner(w), fd(fd) {}
};

typedef struct {
  std::string channel;
  size_t subscribe_num;
} ChannelSubscribeNum;

enum SlowLog {
  kSlowLogMaxArgc = 32,
  kSlowLogMaxString = 128,
};

class Server {
 public:
  explicit Server(Engine::Storage *storage, Config *config);
  ~Server();

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() { return stop_; }
  bool IsLoading() { return is_loading_; }
  Config *GetConfig() { return config_; }
  Status LookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<Redis::Commander> *cmd);


  Status AddMaster(std::string host, uint32_t port);
  Status RemoveMaster();
  Status AddSlave(Redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq);
  void DisconnectSlaves();
  void cleanupExitedSlaves();
  bool IsSlave() { return !master_host_.empty(); }
  void FeedMonitorConns(Redis::Connection *conn, const std::vector<std::string> &tokens);
  void IncrFetchFileThread() { fetch_file_threads_num_++; }
  void DecrFetchFileThread() { fetch_file_threads_num_--; }
  int GetFetchFileThreadNum() { return fetch_file_threads_num_; }

  int PublishMessage(const std::string &channel, const std::string &msg);
  void SubscribeChannel(const std::string &channel, Redis::Connection *conn);
  void UnSubscribeChannel(const std::string &channel, Redis::Connection *conn);
  void GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels);
  void ListChannelSubscribeNum(std::vector<std::string> channels,
                               std::vector<ChannelSubscribeNum> *channel_subscribe_nums);
  void PSubscribeChannel(const std::string &pattern, Redis::Connection *conn);
  void PUnSubscribeChannel(const std::string &pattern, Redis::Connection *conn);
  int GetPubSubPatternSize() { return pubsub_patterns_.size(); }

  void AddBlockingKey(const std::string &key, Redis::Connection *conn);
  void UnBlockingKey(const std::string &key, Redis::Connection *conn);
  Status WakeupBlockingConns(const std::string &key, size_t n_conns);

  std::string GetLastRandomKeyCursor();
  void SetLastRandomKeyCursor(const std::string &cursor);

  static int GetUnixTime();
  void GetStatsInfo(std::string *info);
  void GetServerInfo(std::string *info);
  void GetMemoryInfo(std::string *info);
  void GetRocksDBInfo(std::string *info);
  void GetClientsInfo(std::string *info);
  void GetReplicationInfo(std::string *info);
  void GetRoleInfo(std::string *info);
  void GetCommandsStatsInfo(std::string *info);
  void GetInfo(const std::string &ns, const std::string &section, std::string *info);
  std::string GetRocksDBStatsJson();
  ReplState GetReplicationState();

  void PrepareRestoreDB();
  Status AsyncCompactDB(const std::string &begin_key = "", const std::string &end_key = "");
  Status AsyncBgsaveDB();
  Status AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  Status AsyncScanDBSize(const std::string &ns);
  void GetLastestKeyNumStats(const std::string &ns, KeyNumStats *stats);
  time_t GetLastScanTime(const std::string &ns);

  int DecrClientNum();
  int IncrClientNum();
  int IncrMonitorClientNum();
  int DecrMonitorClientNum();
  std::string GetClientsStr();
  std::atomic<uint64_t> *GetClientID();
  void KillClient(int64_t *killed, std::string addr, uint64_t id, bool skipme, Redis::Connection *conn);
  void SetReplicationRateLimit(uint64_t max_replication_mb);

  LogCollector<PerfEntry> *GetPerfLog() { return &perf_log_; }
  LogCollector<SlowEntry> *GetSlowLog() { return &slow_log_; }
  void SlowlogPushEntryIfNeeded(const std::vector<std::string>* args, uint64_t duration);

  std::unique_ptr<RWLock::ReadLock> WorkConcurrencyGuard();
  std::unique_ptr<RWLock::WriteLock> WorkExclusivityGuard();

  Stats stats_;
  Engine::Storage *storage_;
  Cluster *cluster_;
  static std::atomic<int> unix_time_;

 private:
  void cron();
  void delConnContext(ConnContext *c);
  void updateCachedTime();
  Status autoResizeBlockAndSST();

  std::atomic<bool> stop_;
  std::atomic<bool> is_loading_;
  time_t start_time_ = 0;
  std::mutex slaveof_mu_;
  std::string master_host_;
  uint32_t master_port_ = 0;
  Config *config_ = nullptr;
  std::string last_random_key_cursor_;
  std::mutex last_random_key_cursor_mu_;

  // client counters
  std::atomic<uint64_t> client_id_{1};
  std::atomic<int> connected_clients_{0};
  std::atomic<int> monitor_clients_{0};
  std::atomic<uint64_t> total_clients_{0};
  std::atomic<int> excuting_command_num_{0};

  // slave
  std::mutex slave_threads_mu_;
  std::list<FeedSlaveThread *> slave_threads_;
  std::atomic<int> fetch_file_threads_num_;

  // Some jobs to operate DB should be unique
  std::mutex db_job_mu_;
  bool db_compacting_ = false;
  bool db_bgsave_ = false;
  std::map<std::string, DBScanInfo> db_scan_infos_;

  LogCollector<SlowEntry> slow_log_;
  LogCollector<PerfEntry> perf_log_;

  std::map<ConnContext *, bool> conn_ctxs_;
  std::map<std::string, std::list<ConnContext *>> pubsub_channels_;
  std::map<std::string, std::list<ConnContext *>> pubsub_patterns_;
  std::mutex pubsub_channels_mu_;
  std::map<std::string, std::list<ConnContext *>> blocking_keys_;
  std::mutex blocking_keys_mu_;

  // threads
  RWLock::ReadWriteLock works_concurrency_rw_lock_;
  std::thread cron_thread_;
  std::thread compaction_checker_thread_;
  TaskRunner task_runner_;
  std::vector<WorkerThread *> worker_threads_;
  std::unique_ptr<ReplicationThread> replication_thread_;
};
