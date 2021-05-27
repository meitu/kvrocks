#include "semisync_master.h"

#define SEMISYNC_WAIT_TIMEOUT 10

WaitingNodeManager::WaitingNodeManager() {}

WaitingNodeManager::~WaitingNodeManager() {
  for (std::set<WaitingNode*>::iterator it = waiting_node_list_.begin(); it != waiting_node_list_.end(); it++) {
    delete *it;
  }
  waiting_node_list_.clear();
}

bool WaitingNodeManager::InsertWaitingNode(uint64_t log_file_pos) {
  WaitingNode *ins_node = new WaitingNode();
  ins_node->log_pos = log_file_pos;

  bool is_insert = false;
  if (waiting_node_list_.empty()) {
    is_insert = true;
  } else {
    auto& last_node = *(--waiting_node_list_.end());
    if (last_node->log_pos < log_file_pos) {
      is_insert = true;
    } else {
      auto iter = waiting_node_list_.find(ins_node);
      if (iter == waiting_node_list_.end()) is_insert = true;
    }
  }
  if (is_insert) {
    waiting_node_list_.emplace(ins_node);
  } else {
    LOG(WARNING) << "[semisync] Unknown error to write the same sequence data (" << log_file_pos << ")";
    delete ins_node;
    return false;
  }

  return true;
}

void WaitingNodeManager::ClearWaitingNodes(uint64_t ack_log_file_pos) {
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    auto& item = *iter;
    if (item->log_pos> ack_log_file_pos) break;

    if (item->waiters != 0) {
      iter++;
      continue;
    }
    waiting_node_list_.erase(iter++);
  }
}

// Return the first item whose sequence is greater then or equal to log_file_pos
WaitingNode* WaitingNodeManager::FindWaitingNode(uint64_t log_file_pos) {
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    if ((*iter)->log_pos >= log_file_pos) return *iter;
    iter++;
  }

  if (waiting_node_list_.empty())
    return nullptr;
  else
    return *waiting_node_list_.begin();
}

int WaitingNodeManager::SignalWaitingNodesUpTo(uint64_t log_file_pos) {
  auto iter = waiting_node_list_.begin();
  int ret_num = 0;
  while (iter != waiting_node_list_.end()) {
    if ((*iter)->log_pos > log_file_pos) break;

    (*iter)->cond.notify_all();
    iter++;
    ++ret_num;
  }

  return ret_num;
}

int WaitingNodeManager::SignalWaitingNodesAll() {
  auto iter = waiting_node_list_.begin();
  int ret_num = 0;
  while (iter != waiting_node_list_.end()) {
    (*iter)->cond.notify_all();
    iter++;
    ++ret_num;
  }

  return ret_num;
}

bool AckContainer::Resize(uint32_t size, const AckInfo** ackinfo) {
  if (size == 0) return false;
  if (size - 1 == ack_array_.size()) return true;

  std::vector<AckInfo> old_ack_array;
  old_ack_array.swap(ack_array_);
  ack_array_.resize(size - 1);
  for (auto& info : old_ack_array) {
    if (info.server_id == 0) continue;
    auto* ack = Insert(info.server_id, info.log_pos);
    if (ack) {
      *ackinfo = ack;
    }
  }

  return true;
}

void AckContainer::Clear() {
  for (auto& item : ack_array_) {
    item.reset();
  }
}

void AckContainer::RemoveAll(uint64_t log_file_pos) {
  for (std::size_t i = 0; i < ack_array_.size(); i++) {
    auto& info = ack_array_[i];
    if (info.log_pos == log_file_pos) {
      info.reset();
      empty_slot_ = i;
    }
  }
}

const AckInfo* AckContainer::Insert(int server_id, uint64_t log_file_pos) {
  if (log_file_pos < greatest_return_ack_.log_pos) {
    return nullptr;
  }

  empty_slot_ = ack_array_.size();
  for (std::size_t i = 0; i < ack_array_.size(); i++) {
    auto& info = ack_array_[i];
    if (info.server_id == 0) {
      empty_slot_ = i;
    }
    if (info.server_id == server_id) {
      if (info.log_pos < log_file_pos) {
        info.log_pos = log_file_pos;
      }
      return nullptr;
    }
  }

  AckInfo* ret_ack = nullptr;
  bool to_insert = false;
  if (empty_slot_ == ack_array_.size()) {
    uint64_t min_seq = log_file_pos;
    for (auto& info : ack_array_) {
      if (info.server_id != 0 && info.log_pos < min_seq) {
        min_seq = info.log_pos;
        ret_ack = &info;
      }
    }
    if (ret_ack != nullptr) {
      greatest_return_ack_.set(ret_ack->server_id, ret_ack->log_pos);
    } else {
      greatest_return_ack_.set(server_id, log_file_pos);
    }
    ret_ack = &greatest_return_ack_;
    RemoveAll(greatest_return_ack_.log_pos);

    if (log_file_pos > greatest_return_ack_.log_pos) {
      to_insert = true;
    }
  } else {
    to_insert = true;
  }

  if (to_insert) ack_array_[empty_slot_].set(server_id, log_file_pos);

  return ret_ack;
}

ReplSemiSyncMaster::~ReplSemiSyncMaster() {
  delete node_manager_;
  LOG(INFO) << "exec ReplSemiSyncMaster::~ReplSemiSyncMaster";
}


int ReplSemiSyncMaster::EnableMaster() {
  int result = 0;

  std::lock_guard<std::mutex> lock(LOCK_binlog_);

  if (!GetSemiSyncEnabled()) {
    if (node_manager_ == nullptr)
      node_manager_ = new WaitingNodeManager();

    if (node_manager_ != nullptr) {
      setSemiSyncEnabled(true);
    } else {
      result = -1;
    }
  }

  // initialize state
  if (rpl_semi_sync_master_clients_ < semi_sync_wait_for_slave_count_)
    state_ = false;
  else
    state_ = true;

  return result;
}

int ReplSemiSyncMaster::DisableMaster() {
  std::lock_guard<std::mutex> lock(LOCK_binlog_);

  if (GetSemiSyncEnabled()) {
    switchOff();

    if (node_manager_) {
      delete node_manager_;
      node_manager_ = nullptr;
    }

    setSemiSyncEnabled(false);
    ack_container_.Clear();
  }

  return 0;
}

int ReplSemiSyncMaster::Initalize(Config* config) {
  int result;

  if (init_done_) {
    return 1;
  }
  init_done_ = true;
  config_ = config;
  bool set_result = SetWaitSlaveCount(config_->semi_sync_wait_for_slave_count);
  if (!set_result) {
    LOG(ERROR) << "[semisync] Failed to initialize the semi sync master";
  }
  if (config_->semi_sync_enable)
    result = EnableMaster();
  else
    result = DisableMaster();

  return result;
}

void ReplSemiSyncMaster::AddSlave(FeedSlaveThread* slave_thread_ptr) {
  std::lock_guard<std::mutex> lock(LOCK_binlog_);
  if (slave_thread_ptr == nullptr && slave_thread_ptr->GetConn() == nullptr) {
    LOG(ERROR) << "[semisync] Failed to add slave as semi sync one";
  }
  rpl_semi_sync_master_clients_++;
  slave_threads_.emplace_back(slave_thread_ptr);
}

void ReplSemiSyncMaster::RemoveSlave(FeedSlaveThread* slave_thread_ptr) {
  std::lock_guard<std::mutex> lock(LOCK_binlog_);
  rpl_semi_sync_master_clients_--;
  if (slave_thread_ptr == nullptr && slave_thread_ptr->GetConn() == nullptr) {
    LOG(ERROR) << "[semisync] Failed to remove semi sync slave";
  }
  slave_threads_.remove(slave_thread_ptr);
  if (!GetSemiSyncEnabled() || !IsOn()) return;

  if (rpl_semi_sync_master_clients_ == semi_sync_wait_for_slave_count_ - 1) {
    switchOff();
  }
}

bool ReplSemiSyncMaster::CommitTrx(uint64_t trx_wait_binlog_pos) {
  std::unique_lock<std::mutex> lock(LOCK_binlog_);

  if (!config_->semi_sync_test) {
    for (auto& slave_thread : slave_threads_) {
      slave_thread->Wakeup();
    }
  }

  if (!GetSemiSyncEnabled() || !IsOn()) return false;

  if (trx_wait_binlog_pos <= wait_file_pos_) {
    return false;
  }

  bool insert_result = node_manager_->InsertWaitingNode(trx_wait_binlog_pos);
  if (!insert_result) {
    LOG(ERROR) << "[semisync] Failed to insert log sequence to wait list";
  }
  auto trx_node = node_manager_->FindWaitingNode(trx_wait_binlog_pos);
  if (trx_node == nullptr) {
    LOG(ERROR) << "[semisync] Data in wait list is lost";
    return false;
  }

  trx_node->waiters++;
  auto s = trx_node->cond.wait_for(lock, std::chrono::seconds(SEMISYNC_WAIT_TIMEOUT));
  trx_node->waiters--;
  if (std::cv_status::timeout == s) {
    LOG(ERROR) << "[semisync] Semi sync waits 10s, switch all the slaves to async";
    switchOff();
  }
  if (max_handle_sequence_.load() < trx_wait_binlog_pos) max_handle_sequence_ = trx_wait_binlog_pos;

  if (trx_node->waiters == 0) {
    node_manager_->ClearWaitingNodes(trx_wait_binlog_pos);
  }

  return true;
}

void ReplSemiSyncMaster::HandleAck(int server_id, uint64_t log_file_pos) {
  std::lock_guard<std::mutex> lock(LOCK_binlog_);
  if (semi_sync_wait_for_slave_count_ == 1) {
    reportReplyBinlog(log_file_pos);
  } else {
    auto* ack = ack_container_.Insert(server_id, log_file_pos);
    if (ack != nullptr) {
      reportReplyBinlog(ack->log_pos);
    }
  }
}

void ReplSemiSyncMaster::reportReplyBinlog(uint64_t log_file_pos) {
  if (!GetSemiSyncEnabled()) return;

  if (!IsOn()) {
    trySwitchOn(log_file_pos);
  }

  node_manager_->SignalWaitingNodesUpTo(log_file_pos);
  if (log_file_pos > wait_file_pos_) wait_file_pos_ = log_file_pos;
}

bool ReplSemiSyncMaster::SetWaitSlaveCount(uint new_value) {
  const AckInfo *ackinfo = nullptr;
  std::lock_guard<std::mutex> lock(LOCK_binlog_);
  LOG(INFO) << "[semisync] Try to set slave count " << new_value;

  bool resize_result = ack_container_.Resize(new_value, &ackinfo);
  if (resize_result) {
    if (ackinfo != nullptr) {
      reportReplyBinlog(ackinfo->log_pos);
    }
    semi_sync_wait_for_slave_count_ = new_value;
  }

  LOG(INFO) << "[semisync] Finish setting slave count";

  return resize_result;
}

void ReplSemiSyncMaster::trySwitchOn(uint64_t log_file_pos) {
  if (semi_sync_enabled_) {
    if (log_file_pos > max_handle_sequence_) {
      state_ = true;
    }
  }
}

void ReplSemiSyncMaster::switchOff() {
  state_ = false;
  wait_file_pos_ = 0;
  max_handle_sequence_ = 0;

  node_manager_->SignalWaitingNodesAll();
}

// semisync_master.cc
