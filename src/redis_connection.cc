#include <rocksdb/perf_context.h>
#include <rocksdb/iostats_context.h>
#include <glog/logging.h>

#include "redis_connection.h"
#include "worker.h"
#include "server.h"

namespace Redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : bev_(bev), req_(owner->svr_), owner_(owner), svr_(owner->svr_) {
  time_t now;
  time(&now);
  create_time_ = now;
  last_interaction_ = now;
  need_close_ = true;
}

Connection::~Connection() {
  int fd = bufferevent_getfd(bev_);
  if (bev_) { bufferevent_free(bev_); }
  if (need_close_) close(fd);
  // unscribe all channels and patterns if exists
  UnSubscribeAll();
  PUnSubscribeAll();
}

std::string Connection::ToString() {
  std::ostringstream stream;
  stream << "id=" << id_
    << " addr=" << addr_
    << " fd=" << bufferevent_getfd(bev_)
    << " name=" << name_
    << " age=" << GetAge()
    << " idle=" << GetIdleTime()
    << " flags=" << GetFlags()
    << " namespace=" << ns_
    << " qbuf=" << evbuffer_get_length(Input())
    << " obuf=" << evbuffer_get_length(Output())
    << " cmd=" << last_cmd_
    << "\n";
  return stream.str();
}

void Connection::Close() {
  owner_->FreeConnection(this);
}

void Connection::Detach() {
  owner_->DetachConnection(this);
}

void Connection::OnRead(struct bufferevent *bev, void *ctx) {
  DLOG(INFO) << "[connection] on read: " << bufferevent_getfd(bev);
  auto conn = static_cast<Connection *>(ctx);

  conn->SetLastInteraction();
  auto s = conn->req_.Tokenize(conn->Input());
  if (!s.IsOK()) {
    conn->EnableFlag(Redis::Connection::kCloseAfterReply);
    conn->Reply(Redis::Error(s.Msg()));
    LOG(INFO) << "Failed to tokenize the request, encounter error: " << s.Msg();
    return;
  }
  conn->ExecuteCommands(conn->req_.GetCommands());
  conn->req_.ClearCommands();
  if (conn->IsFlagEnabled(kCloseAsync)) {
    conn->Close();
  }
}

void Connection::OnWrite(struct bufferevent *bev, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (conn->IsFlagEnabled(kCloseAfterReply) ||
      conn->IsFlagEnabled(kCloseAsync)) {
    conn->Close();
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events, void *ctx) {
  auto conn = static_cast<Connection *>(ctx);
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "[connection] Going to remove the client: " << conn->GetAddr()
               << ", while encounter error: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    conn->Close();
    return;
  }
  if (events & BEV_EVENT_EOF) {
    DLOG(INFO) << "[connection] Going to remove the client: " << conn->GetAddr()
               << ", while closed by client";
    conn->Close();
    return;
  }
  if (events & BEV_EVENT_TIMEOUT) {
    DLOG(INFO) << "[connection] The client: " << conn->GetAddr()  << "] reached timeout";
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  owner_->svr_->stats_.IncrOutbondBytes(msg.size());
  Redis::Reply(bufferevent_get_output(bev_), msg);
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, int port) {
  ip_ = std::move(ip);
  port_ = port;
  addr_ = ip_ +":"+ std::to_string(port_);
}

uint64_t Connection::GetAge() {
  time_t now;
  time(&now);
  return static_cast<uint64_t>(now-create_time_);
}

void Connection::SetLastInteraction() {
  time(&last_interaction_);
}

uint64_t Connection::GetIdleTime() {
  time_t now;
  time(&now);
  return static_cast<uint64_t>(now-last_interaction_);
}

std::string Connection::GetFlags() {
  std::string flags;
  if (owner_->IsRepl()) flags.append("R");
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (!subscribe_channels_.empty()) flags.append("P");
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) {
  flags_ |= flag;
}

void Connection::DisableFlag(Flag flag) {
  flags_ &= (~flag);
}

bool Connection::IsFlagEnabled(Flag flag) {
  return (flags_ & flag) > 0;
}

bool Connection::IsRepl() {
  return owner_->IsRepl();
}

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }
  subscribe_channels_.emplace_back(channel);
  owner_->svr_->SubscribeChannel(channel, this);
}

void Connection::UnSubscribeChannel(const std::string &channel) {
  auto iter = subscribe_channels_.begin();
  for (; iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->svr_->UnSubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnSubscribeAll(unsubscribe_callback reply) {
  if (subscribe_channels_.empty()) {
    if (reply != nullptr) reply("", subcribe_patterns_.size());
    return;
  }
  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->svr_->UnSubscribeChannel(chan, this);
    removed++;
    if (reply != nullptr) {
      reply(chan, static_cast<int>(subscribe_channels_.size() -
                                   removed + subcribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() {
  return static_cast<int>(subscribe_channels_.size());
}

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subcribe_patterns_) {
    if (pattern == p) return;
  }
  subcribe_patterns_.emplace_back(pattern);
  owner_->svr_->PSubscribeChannel(pattern, this);
}

void Connection::PUnSubscribeChannel(const std::string &pattern) {
  auto iter = subcribe_patterns_.begin();
  for (; iter != subcribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subcribe_patterns_.erase(iter);
      owner_->svr_->PUnSubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnSubscribeAll(unsubscribe_callback reply) {
  if (subcribe_patterns_.empty()) {
    if (reply != nullptr) reply("", subscribe_channels_.size());
    return;
  }

  int removed = 0;
  for (const auto &pattern : subcribe_patterns_) {
    owner_->svr_->PUnSubscribeChannel(pattern, this);
    removed++;
    if (reply != nullptr) {
      reply(pattern, static_cast<int>(subcribe_patterns_.size() -
                                      removed + subscribe_channels_.size()));
    }
  }
  subcribe_patterns_.clear();
}

int Connection::PSubscriptionsCount() {
  return static_cast<int>(subcribe_patterns_.size());
}

bool Connection::isProfilingEnabled(const std::string &cmd) {
  auto config = svr_->GetConfig();
  if (config->profiling_sample_ratio == 0) return false;
  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }
  if (config->profiling_sample_ratio == 100 ||
      std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }
  return false;
}

void Connection::recordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration) {
  int threshold = svr_->GetConfig()->profiling_sample_record_threshold_ms;
  if (threshold > 0 && static_cast<int>(duration/1000) < threshold) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  std::string perf_context = rocksdb::get_perf_context()->ToString(true);
  std::string iostats_context = rocksdb::get_iostats_context()->ToString(true);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  if (perf_context.empty()) return;  // request without db operation
  auto entry = new PerfEntry();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->iostats_context = std::move(iostats_context);
  entry->perf_context = std::move(perf_context);
  svr_->GetPerfLog()->PushEntry(entry);
}

void Connection::ExecuteCommands(const std::vector<Redis::CommandTokens> &to_process_cmds) {
  if (to_process_cmds.empty()) return;

  Config *config = svr_->GetConfig();
  std::string reply, password;
  password = IsRepl() ? config->masterauth : config->requirepass;

  for (auto &cmd_tokens : to_process_cmds) {
    if (IsFlagEnabled(Redis::Connection::kCloseAfterReply)) break;
    if (GetNamespace().empty()) {
      if (!password.empty() && Util::ToLower(cmd_tokens.front()) != "auth") {
        Reply(Redis::Error("NOAUTH Authentication required."));
        continue;
      }
      if (password.empty()) {
        BecomeAdmin();
        SetNamespace(kDefaultNamespace);
      }
    }

    auto s = svr_->LookupAndCreateCommand(cmd_tokens.front(), &current_cmd_);
    if (!s.IsOK()) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error("ERR unknown command"));
      continue;
    }
    const auto attributes = current_cmd_->GetAttributes();
    auto cmd_name = attributes->name;

    std::unique_ptr<RWLock::ReadLock>  concurrency;  // Allow concurrency
    std::unique_ptr<RWLock::WriteLock> exclusivity;  // Need exclusivity
    // If the command need to process exclusively, we need to get 'ExclusivityGuard'
    // that can guarantee other threads can't come into critical zone, such as DEBUG,
    // CLUSTER subcommand, CONFIG SET, MULTI, LUA (in the immediate future).
    // Otherwise, we just use 'ConcurrencyGuard' to allow all workers to execute
    // commands at the same time.
    if (IsFlagEnabled(Connection::kMultiExec) && attributes->name != "exec") {
      // No lock guard, because 'exec' command has acquired 'WorkExclusivityGuard'
    } else if (attributes->is_exclusive() ||
        (cmd_name == "config" && cmd_tokens.size() == 2 && !strcasecmp(cmd_tokens[1].c_str(), "set")) ||
        (config->cluster_enabled && cmd_name == "clusterx" && cmd_tokens.size() >= 2
         && Cluster::SubCommandIsExecExclusive(cmd_tokens[1]))) {
      exclusivity = svr_->WorkExclusivityGuard();
    } else {
      concurrency = svr_->WorkConcurrencyGuard();
    }

    if (svr_->IsLoading() && attributes->is_ok_loading() == false) {
      Reply(Redis::Error("LOADING kvrocks is restoring the db from backup"));
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      continue;
    }
    int arity = attributes->arity;
    int tokens = static_cast<int>(cmd_tokens.size());
    if ((arity > 0 && tokens != arity)
        || (arity < 0 && tokens < -arity)) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error("ERR wrong number of arguments"));
      continue;
    }
    current_cmd_->SetArgs(cmd_tokens);
    s = current_cmd_->Parse(cmd_tokens);
    if (!s.IsOK()) {
      if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
      Reply(Redis::Error(s.Msg()));
      continue;
    }

    if (IsFlagEnabled(Connection::kMultiExec) && attributes->is_no_multi()) {
      std::string no_multi_err = "Err Can't execute " + attributes->name + " in MULTI";
      Reply(Redis::Error(no_multi_err));
      multi_error_ = true;
      continue;
    }

    if (config->cluster_enabled) {
      s = svr_->cluster_->CanExecByMySelf(attributes, cmd_tokens);
      if (!s.IsOK()) {
        if (IsFlagEnabled(Connection::kMultiExec)) multi_error_ = true;
        Reply(Redis::Error(s.Msg()));
        continue;;
      }
    }

    // We don't execute commands, but queue them, ant then execute in EXEC command
    if (IsFlagEnabled(Connection::kMultiExec) && !in_exec_ && !attributes->is_multi()) {
      multi_cmds_.emplace_back(cmd_tokens);
      Reply(Redis::SimpleString("QUEUED"));
      continue;
    }

    if (config->slave_readonly && svr_->IsSlave() && attributes->is_write()) {
      Reply(Redis::Error("READONLY You can't write against a read only slave."));
      continue;
    }
    if (!config->slave_serve_stale_data && svr_->IsSlave()
        && cmd_name != "info" && cmd_name != "slaveof"
        && svr_->GetReplicationState() != kReplConnected) {
      Reply(Redis::Error("MASTERDOWN Link with MASTER is down "
                               "and slave-serve-stale-data is set to 'no'."));
      continue;
    }

    SetLastCmd(cmd_name);
    svr_->stats_.IncrCalls(cmd_name);
    auto start = std::chrono::high_resolution_clock::now();
    bool is_profiling = isProfilingEnabled(cmd_name);
    s = current_cmd_->Execute(svr_, this, &reply);
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    if (is_profiling) recordProfilingSampleIfNeed(cmd_name, duration);
    svr_->SlowlogPushEntryIfNeeded(current_cmd_->Args(), duration);
    svr_->stats_.IncrLatency(static_cast<uint64_t>(duration), cmd_name);
    svr_->FeedMonitorConns(this, cmd_tokens);
    // Reply for MULTI
    if (!s.IsOK()) {
      Reply(Redis::Error("ERR " + s.Msg()));
      continue;
    }
    if (!reply.empty()) Reply(reply);
    reply.clear();
  }
}

void Connection::ResetMultiExec() {
  in_exec_ = false;
  multi_error_ = false;
  multi_cmds_.clear();
  DisableFlag(Connection::kMultiExec);
}

}  // namespace Redis
