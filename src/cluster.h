#pragma once

#include <string>
#include <vector>
#include <bitset>
#include <memory>
#include <algorithm>
#include <unordered_map>

#include "status.h"
#include "rw_lock.h"
#include "redis_cmd.h"
#include "redis_slot.h"

enum {
  kClusterMaster   = 1,
  kClusterSlave    = 2,
  kClusetNodeIdLen = 40,
  kClusterPortIncr = 10000,
  kClusterSlots    = HASH_SLOTS_SIZE,
};

class ClusterNode {
 public:
  explicit ClusterNode(std::string id, std::string host, int port,
        int role, std::string master_id, std::bitset<kClusterSlots> slots);
  std::string id_;
  std::string host_;
  int port_;
  int role_;
  std::string master_id_;
  std::string slots_info_;
  std::bitset<kClusterSlots> slots_;
  std::vector<std::string> replicas;
};

struct SlotInfo {
  int start;
  int end;
  struct NodeInfo {
    std::string host;
    int port;
    std::string id;
  };
  std::vector<NodeInfo> nodes;
};

typedef std::unordered_map<std::string, std::shared_ptr<ClusterNode>> ClusterNodes;

class Cluster {
 public:
  explicit Cluster(std::vector<std::string> binds, int port);
  Status SetClusterNodes(const std::string &nodes_str, int64_t version, bool force);
  Status GetClusterNodes(std::string *nodes_str);
  Status SetNodeId(std::string node_id);
  Status GetSlotsInfo(std::vector<SlotInfo> *slot_infos);
  Status GetClusterInfo(std::string *cluster_infos);
  uint64_t GetVersion() { return version_; }
  bool IsValidSlot(int slot) { return slot >= 0 && slot < kClusterSlots; }
  Status CanExecByMySelf(const Redis::CommandAttributes *attributes,
                         const std::vector<std::string> &cmd_tokens);

  static bool SubCommandIsExecExclusive(const std::string &subcommand);

 private:
  std::string GenNodesDescription();
  SlotInfo GenSlotNodeInfo(int start, int end, std::shared_ptr<ClusterNode> n);
  Status ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                    std::unordered_map<int, std::string> *slots_nodes);
  std::vector<std::string> binds_;
  int port_;
  int size_;
  int64_t version_;
  std::string  myid_;
  std::shared_ptr<ClusterNode> myself_;
  ClusterNodes nodes_;
  std::shared_ptr<ClusterNode> slots_nodes_[kClusterSlots];
};
