#pragma once

#include <vector>
#include <string>
#include <event2/buffer.h>

#include "status.h"

class Server;

namespace Redis {

using CommandTokens = std::vector<std::string>;

class Connection;

class Request {
 public:
  explicit Request(Server *svr) : svr_(svr) {}
  // Not copyable
  Request(const Request &) = delete;
  Request &operator=(const Request &) = delete;

  // Parse the redis requests (bulk string array format)
  Status Tokenize(evbuffer *input);

  const std::vector<CommandTokens> &GetCommands() { return commands_; }
  void ClearCommands() { commands_.clear(); }

 private:
  // internal states related to parsing

  enum ParserState { ArrayLen, BulkLen, BulkData };
  ParserState state_ = ArrayLen;
  size_t multi_bulk_len_ = 0;
  size_t bulk_len_ = 0;
  CommandTokens tokens_;
  std::vector<CommandTokens> commands_;

  Server *svr_;
};

}  // namespace Redis
