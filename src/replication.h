#pragma once

#include <thread>
#include <vector>
#include <utility>
#include <memory>
#include <tuple>
#include <string>
#include <deque>
#include <condition_variable>
#include <event2/bufferevent.h>

#include "status.h"
#include "storage.h"
#include "redis_connection.h"
#include "observer.h"

class Server;

enum ReplState {
  kReplConnecting = 1,
  kReplSendAuth,
  kReplCheckDBName,
  kReplReplConf,
  kReplSendPSync,
  kReplFetchMeta,
  kReplFetchSST,
  kReplConnected,
  kReplError,
};

typedef std::function<void(const std::string, const uint32_t)> fetch_file_callback;
class FeedSlaveThread;

class TransmitStartEvent : public ObserverEvent {
 public:
  explicit TransmitStartEvent(int fd, FeedSlaveThread* target_thread) :
    ObserverEvent(), conn_fd(fd), thread_ptr(target_thread) {}
  int conn_fd;
  FeedSlaveThread* thread_ptr;
};

class BeforeSendEvent : public ObserverEvent {};

class AfterSendEvent : public ObserverEvent {
 public:
  explicit AfterSendEvent(int fd, uint64_t sequence) :
    ObserverEvent(), conn_fd(fd), sequenceNumber(sequence) {}
  int conn_fd;
  uint64_t sequenceNumber;
};

class TransmitEndEvent : public ObserverEvent {
 public:
  explicit TransmitEndEvent(FeedSlaveThread* target_thread) :
    ObserverEvent(), thread_ptr(target_thread) {}
  FeedSlaveThread* thread_ptr;
};

class SyncStatusChangeEvent : public ObserverEvent {
 public:
  SyncStatusChangeEvent() : ObserverEvent() {}
  int conn_fd = 0;
  uint64_t lastSequenceNumberEnd = 0;
};

class FeedSlaveHandler : public EventHandler {
 public:
  FeedSlaveHandler() : EventHandler() {
    registerEventHandler<TransmitStartEvent>(
      std::bind(&FeedSlaveHandler::transmitStart, this, std::placeholders::_1, std::placeholders::_2));
    registerEventHandler<BeforeSendEvent>(
      std::bind(&FeedSlaveHandler::beforeSend, this, std::placeholders::_1, std::placeholders::_2));
    registerEventHandler<AfterSendEvent>(
      std::bind(&FeedSlaveHandler::afterSend, this, std::placeholders::_1, std::placeholders::_2));
    registerEventHandler<TransmitEndEvent>(
      std::bind(&FeedSlaveHandler::transmitEnd, this, std::placeholders::_1, std::placeholders::_2));
  }

 private:
  void transmitStart(Observable subject, ObserverEvent const& event);
  void beforeSend(Observable subject, ObserverEvent const& event);
  void afterSend(Observable subject, ObserverEvent const& event);
  void transmitEnd(Observable subject, ObserverEvent const& event);
};

class FeedStatusHandler : public EventHandler {
 public:
  FeedStatusHandler() : EventHandler() {
    registerEventHandler<SyncStatusChangeEvent>(
      std::bind(&FeedStatusHandler::sync_status_change, this, std::placeholders::_1, std::placeholders::_2));
  }

 private:
  void sync_status_change(Observable subject, ObserverEvent const& event);
};

class FeedSlaveThread : public Observable {
 public:
  explicit FeedSlaveThread(Server *srv, Redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq)
      : srv_(srv), conn_(conn), next_repl_seq_(next_repl_seq) {
        RegisterObserver(handler_.get());
        RegisterObserver(handler2_.get());
      }
  ~FeedSlaveThread();

  Status Start();
  void Stop();
  void Join();
  void Pause(const uint32_t& micro_time_out);
  void Wakeup();
  bool IsStopped() { return stop_; }
  Redis::Connection *GetConn() { return conn_; }
  rocksdb::SequenceNumber GetCurrentReplSeq() { return next_repl_seq_ == 0 ? 0 : next_repl_seq_-1; }

 private:
  static const std::unique_ptr<FeedSlaveHandler> handler_;
  static const std::unique_ptr<FeedStatusHandler> handler2_;
  uint64_t interval = 0;
  bool stop_ = false;
  bool pause_ = false;
  Server *srv_ = nullptr;
  Redis::Connection *conn_ = nullptr;
  rocksdb::SequenceNumber next_repl_seq_ = 0;
  std::thread t_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter_ = nullptr;
  std::mutex mutex_;
  std::condition_variable cv_;


  void loop();
  void checkLivenessIfNeed();
};

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port,
                             Server *srv, std::string auth = "");
  Status Start(std::function<void()> &&pre_fullsync_cb,
               std::function<void()> &&post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_; }
  time_t LastIOTime() { return last_io_time_; }

 protected:
  event_base *base_ = nullptr;

  // The state machine to manage the asynchronous steps used in replication
  class CallbacksStateMachine {
   public:
    enum class State {
      NEXT,
      AGAIN,
      QUIT,
      RESTART,
    };
    enum EventType {
      READ,
      WRITE,
    };
    using CallbackType = std::tuple<EventType, std::string, std::function<State(bufferevent *, void *)>>;
    using CallbackList = std::deque<CallbackType>;
    CallbacksStateMachine(ReplicationThread *repl, CallbackList &&handlers);

    void Start();
    void Stop();
    static void EvCallback(bufferevent *bev, void *ctx);
    static void ConnEventCB(bufferevent *bev, int16_t events,
                            void *state_machine_ptr);
    static void SetReadCB(bufferevent *bev, bufferevent_data_cb cb,
                          void *state_machine_ptr);
    static void SetWriteCB(bufferevent *bev, bufferevent_data_cb cb,
                           void *state_machine_ptr);

   private:
    bufferevent *bev_ = nullptr;
    ReplicationThread *repl_;
    CallbackList handlers_;
    CallbackList::size_type handler_idx_ = 0;

    EventType getHandlerEventType(CallbackList::size_type idx) {
      return std::get<0>(handlers_[idx]);
    }
    std::string getHandlerName(CallbackList::size_type idx) {
      return std::get<1>(handlers_[idx]);
    }
    std::function<State(bufferevent *, void *)> getHandlerFunc(CallbackList::size_type idx) {
      return std::get<2>(handlers_[idx]);
    }
  };

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  std::string auth_;
  Server *srv_ = nullptr;
  Engine::Storage *storage_ = nullptr;
  ReplState repl_state_;
  time_t last_io_time_ = 0;

  std::function<void()> pre_fullsync_cb_;
  std::function<void()> post_fullsync_cb_;

  // Internal states managed by FullSync procedure
  enum FullSyncState {
    kFetchMetaID,
    kFetchMetaSize,
    kFetchMetaContent,
  } fullsync_state_ = kFetchMetaID;
  rocksdb::BackupID fullsync_meta_id_ = 0;
  size_t fullsync_filesize_ = 0;

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;

  size_t incr_bulk_len_ = 0;

  using CBState = CallbacksStateMachine::State;
  CallbacksStateMachine psync_steps_;
  CallbacksStateMachine fullsync_steps_;

  void run();

  static CBState authWriteCB(bufferevent *bev, void *ctx);
  static CBState authReadCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameWriteCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameReadCB(bufferevent *bev, void *ctx);
  static CBState replConfWriteCB(bufferevent *bev, void *ctx);
  static CBState replConfReadCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncReadCB(bufferevent *bev, void *ctx);
  static CBState incrementBatchLoopCB(bufferevent *bev, void *ctx);
  static CBState fullSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState fullSyncReadCB(bufferevent *bev, void *ctx);

  // Synchronized-Blocking ops
  Status sendAuth(int sock_fd);
  Status fetchFile(int sock_fd, evbuffer *evbuf, const std::string &dir,
                   const std::string file, uint32_t crc, fetch_file_callback fn);
  Status fetchFiles(int sock_fd, const std::string &dir,
                  const std::vector<std::string> &files,
                  const std::vector<uint32_t> &crcs,
                  fetch_file_callback fn);
  Status parallelFetchFile(const std::string &dir,
                  const std::vector<std::pair<std::string, uint32_t>> &files);
  static bool isRestoringError(const char *err);

  static void EventTimerCB(int, int16_t, void *ctx);

  rocksdb::Status ParseWriteBatch(const std::string &batch_string);
};

/*
 * An extractor to extract update from raw writebatch
 */
class WriteBatchHandler : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice &key,
                        const rocksdb::Slice &value) override;

  rocksdb::Slice GetPublishChannel() { return publish_message_.first; }
  rocksdb::Slice GetPublishValue() { return publish_message_.second; }
  bool IsPublish() { return is_publish_; }
 private:
  std::pair<std::string, std::string> publish_message_;
  bool is_publish_ = false;
};
