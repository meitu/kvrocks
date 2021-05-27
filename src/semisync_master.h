#pragma once

#include <thread>
#include <vector>
#include <memory>
#include <string>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <set>
#include <list>
#include <event2/bufferevent.h>

#include "replication.h"
#include "config.h"

struct WaitingNode {
  uint64_t log_pos;
  std::condition_variable cond;
  int waiters;
};

struct WaitingNodeComparator {
  bool operator()(struct WaitingNode* left, struct WaitingNode* right) const {
    if (left->log_pos == right->log_pos) return false;

    return left->log_pos < right->log_pos;
  }
};

struct AckInfo {
  int server_id = 0;
  uint64_t log_pos = 0;
  void reset() { server_id = 0; log_pos = 0; }
  void set(int id, uint64_t pos) {server_id = id; log_pos = pos; }
};

class WaitingNodeManager {
 public:
  WaitingNodeManager();
  ~WaitingNodeManager();
  bool InsertWaitingNode(uint64_t log_file_pos);
  void ClearWaitingNodes(uint64_t ack_log_file_pos);
  WaitingNode* FindWaitingNode(uint64_t log_file_pos);
  int SignalWaitingNodesUpTo(uint64_t log_file_pos);
  int SignalWaitingNodesAll();

 private:
  std::set<WaitingNode*, WaitingNodeComparator> waiting_node_list_;
};

class AckContainer {
 public:
  AckContainer() {}
  ~AckContainer() = default;
  bool Resize(uint32_t size, const AckInfo** ackinfo);
  void Clear();
  void RemoveAll(uint64_t log_file_pos);
  const AckInfo *Insert(int server_id, uint64_t log_file_pos);

 private:
  AckContainer(AckContainer const &container);
  AckContainer &operator=(const AckContainer &container);
  std::vector<AckInfo> ack_array_;
  AckInfo greatest_return_ack_;
  uint32_t empty_slot_ = 0;
};

class ReplSemiSyncMaster {
 public:
  ReplSemiSyncMaster(const ReplSemiSyncMaster&) = delete;
  ReplSemiSyncMaster& operator=(const ReplSemiSyncMaster&) = delete;
  static ReplSemiSyncMaster& GetInstance() {
    static ReplSemiSyncMaster instance;
    return instance;
  }
  ~ReplSemiSyncMaster();
  bool InitDone() { return init_done_.load(); }
  int Initalize(Config* config);
  bool GetSemiSyncEnabled() { return semi_sync_enabled_; }
  bool IsOn() { return (state_); }
  int EnableMaster();
  int DisableMaster();
  void AddSlave(FeedSlaveThread* slave_thread_ptr);
  void RemoveSlave(FeedSlaveThread* slave_thread_ptr);
  bool CommitTrx(uint64_t trx_wait_binlog_pos);
  void HandleAck(int server_id, uint64_t log_file_pos);
  bool SetWaitSlaveCount(uint new_value);

 private:
  ReplSemiSyncMaster() {}
  Config* config_ = nullptr;
  int rpl_semi_sync_master_clients_ = 0;
  int semi_sync_wait_for_slave_count_ = 1;
  std::list<FeedSlaveThread*> slave_threads_;
  WaitingNodeManager* node_manager_ = nullptr;
  AckContainer ack_container_;
  std::mutex LOCK_binlog_;
  uint64_t wait_file_pos_ = 0;
  std::atomic<bool> init_done_ = {false};
  bool state_ = false; /* whether semi-sync is switched */
  std::atomic<bool> semi_sync_enabled_ = {false};        /* semi-sync is enabled on the master */
  std::atomic<uint64_t> max_handle_sequence_ = {0};

  void setSemiSyncEnabled(bool enabled) { semi_sync_enabled_ = enabled; }
  void switchOff();
  void trySwitchOn(uint64_t log_file_pos);
  void reportReplyBinlog(uint64_t log_file_pos);
};

// semisync_master

