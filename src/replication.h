#pragma once

#include <thread>
#include <event2/bufferevent.h>

#include "status.h"
#include "storage.h"
typedef enum {
  kReplConnecting = 1,
  kReplCheckDBName,
  kReplSendPSync,
  kReplFetchMeta,
  kReplFetchSST,
  kReplConnected
}ReplState;

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port, Engine::Storage *storage);
  void Start(std::function<void()>&& pre_fullsync_cb, std::function<void()>&& post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_; }
  rocksdb::SequenceNumber Offset() { return seq_; }

 private:
  std::thread t_;
  event_base *base_;
  bufferevent *upstream_bev_;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  Engine::Storage *storage_;
  rocksdb::SequenceNumber seq_ = 0;
  ReplState repl_state_;
  Status last_status = Status::OK(); // Use to indicate some fatal errors

  std::function<void()> pre_fullsync_cb_;
  std::function<void()> post_fullsync_cb_;

  // Internal states managed by FullSync procedure
  enum FullSyncState {
    Fetch_meta_id,
    Fetch_meta_size,
    Fetch_meta_content,
  } fullsync_state_ = Fetch_meta_id;
  rocksdb::BackupID fullsync_meta_id_ = 0;
  size_t fullsync_filesize_ = 0;

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;
  size_t incr_bulk_len_ = 0;

  void Run();
  static void CheckDBName_write_cb(bufferevent *bev, void *ctx);
  static void CheckDBName_read_cb(bufferevent *bev, void *ctx);
  static void TryPsync_write_cb(bufferevent *bev, void *ctx);
  static void TryPsync_read_cb(bufferevent *bev, void *ctx);
  static void IncrementBatchLoop_cb(bufferevent *bev, void *ctx);
  static void FullSync_write_cb(bufferevent *bev, void *ctx);
  static void FullSync_read_cb(bufferevent *bev, void *ctx);
  Status FetchFile(int sock_fd, std::string path, uint32_t crc);

  static void Timer_cb(int, short, void *ctx);
};