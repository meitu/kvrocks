#include <chrono>
#include <utility>
#include <memory>
#include <glog/logging.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/iostats_context.h>

#include "util.h"
#include "redis_cmd.h"
#include "redis_reply.h"
#include "redis_request.h"
#include "redis_connection.h"
#include "server.h"

namespace Redis {
const size_t PROTO_INLINE_MAX_SIZE = 16 * 1024L;
const size_t PROTO_BULK_MAX_SIZE = 512 * 1024L * 1024L;
const size_t PROTO_MULTI_MAX_SIZE = 1024 * 1024L;

Status Request::Tokenize(evbuffer *input) {
  char *line;
  size_t len;
  size_t pipeline_size = 0;
  while (true) {
    switch (state_) {
      case ArrayLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line || len <= 0) {
          if (pipeline_size > 128) {
            LOG(INFO) << "Large pipeline detected: " << pipeline_size;
          }
          if (line) {
            free(line);
            continue;
          }
          return Status::OK();
        }
        pipeline_size++;
        svr_->stats_.IncrInbondBytes(len);
        if (line[0] == '*') {
          try {
            multi_bulk_len_ = std::stoull(std::string(line + 1, len-1));
          } catch (std::exception &e) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          if (multi_bulk_len_ > PROTO_MULTI_MAX_SIZE) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          state_ = BulkLen;
        } else {
          if (len > PROTO_INLINE_MAX_SIZE) {
            free(line);
            return Status(Status::NotOK, "Protocol error: invalid bulk length");
          }
          Util::Split(std::string(line, len), " \t", &tokens_);
          commands_.emplace_back(std::move(tokens_));
          state_ = ArrayLen;
        }
        free(line);
        break;
      case BulkLen:
        line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF_STRICT);
        if (!line || len <= 0) return Status::OK();
        svr_->stats_.IncrInbondBytes(len);
        if (line[0] != '$') {
          free(line);
          return Status(Status::NotOK, "Protocol error: expected '$'");
        }
        try {
          bulk_len_ = std::stoull(std::string(line + 1, len-1));
        } catch (std::exception &e) {
          free(line);
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        if (bulk_len_ > PROTO_BULK_MAX_SIZE) {
          free(line);
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        free(line);
        state_ = BulkData;
        break;
      case BulkData:
        if (evbuffer_get_length(input) < bulk_len_ + 2) return Status::OK();
        char *data = reinterpret_cast<char *>(evbuffer_pullup(input, bulk_len_ + 2));
        tokens_.emplace_back(data, bulk_len_);
        evbuffer_drain(input, bulk_len_ + 2);
        svr_->stats_.IncrInbondBytes(bulk_len_ + 2);
        --multi_bulk_len_;
        if (multi_bulk_len_ == 0) {
          state_ = ArrayLen;
          commands_.emplace_back(std::move(tokens_));
          tokens_.clear();
        } else {
          state_ = BulkLen;
        }
        break;
    }
  }
}

}  // namespace Redis
