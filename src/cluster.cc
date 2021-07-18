#include <cassert>
#include <cstring>
#include <algorithm>

#include "util.h"
#include "cluster.h"
#include "redis_cmd.h"

ClusterNode::ClusterNode(std::string id, std::string host, int port,
    int role, std::string master_id, std::bitset<kClusterSlots> slots):
    id_(id), host_(host), port_(port), role_(role),
    master_id_(master_id), slots_(slots) { }

Cluster::Cluster(std::vector<std::string> binds, int port) :
    binds_(binds), port_(port), size_(0), version_(-1), myself_(nullptr) {
  for (unsigned i = 0; i < kClusterSlots; i++) {
    slots_nodes_[i] = nullptr;
  }
}

// We access cluster without lock, acutally we guarantte data-safe by work theads
// ReadWriteLockGuard, CLUSTER command doesn't have 'execlusive' attribute, i.e.
// CLUSTER command can be executed concurrently, but some subcommand may change
// cluster data, so these commands should be executed exclusively, and ReadWriteLock
// also can guarantte accessing data is safe.
bool Cluster::SubCommandIsExecExclusive(const std::string &subcommand) {
  if (strcasecmp("setnodes", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("setnodeid", subcommand.c_str()) == 0) {
    return true;
  }
  return false;
}

Status Cluster::SetNodeId(std::string node_id) {
  if (node_id.size() != kClusetNodeIdLen) {
    return Status(Status::ClusterInvalidInfo, "Invalid node id");
  }

  myid_ = node_id;
  // Already has cluster topology
  if (version_ >= 0 && nodes_.find(node_id) != nodes_.end()) {
    myself_ = nodes_[myid_];
  } else {
    myself_ = nullptr;
  }
  return Status::OK();
}

// cluster setnodes $all_nodes_info $version $force
// one line of $all_nodes: $node_id $host $port $role $master_node_id $slot_range
Status Cluster::SetClusterNodes(const std::string &nodes_str, int64_t version, bool force) {
  if (version < 0) return Status(Status::NotOK, "Invalid version");

  if (force == false) {
    // Low version wants to reset current version
    if (version_ > version) {
      return Status(Status::NotOK, "Invalid version of cluster");
    }
    // The same version, it is not needed to update
    if (version_ == version) return Status::OK();
  }

  ClusterNodes nodes;
  std::unordered_map<int, std::string> slots_nodes;
  Status s = ParseClusterNodes(nodes_str, &nodes, &slots_nodes);
  if (!s.IsOK()) return s;

  // Update version and cluster topology
  version_ = version;
  nodes_ = nodes;
  size_ = 0;

  // Update slots to nodes
  for (const auto &n : slots_nodes) {
    slots_nodes_[n.first] = nodes_[n.second];
  }

  // Update replicas info and size
  for (auto &n : nodes_) {
    if (n.second->role_ == kClusterSlave) {
      if (nodes_.find(n.second->master_id_) != nodes.end()) {
        nodes_[n.second->master_id_]->replicas.push_back(n.first);
      }
    }
    if (n.second->role_ == kClusterMaster && n.second->slots_.count() > 0) {
      size_++;
    }
  }

  // Find myself
  if (myid_.empty() || force) {
    for (auto &n : nodes_) {
      if (n.second->port_ == port_ &&
          std::find(binds_.begin(), binds_.end(), n.second->host_)
            != binds_.end()) {
        myid_ = n.first;
        break;
      }
    }
  }
  myself_ = nullptr;
  if (!myid_.empty() && nodes_.find(myid_) != nodes_.end()) {
    myself_ = nodes_[myid_];
  }

  return Status::OK();
}

Status Cluster::GetClusterInfo(std::string *cluster_infos) {
  if (version_ < 0) {
    return Status(Status::ClusterDown,
      "CLUSTERDOWN The cluster is not initialized");
  }
  cluster_infos->clear();

  int ok_slot = 0;
  for (int i = 0; i < kClusterSlots; i++) {
    if (slots_nodes_[i] != nullptr) ok_slot++;
  }

  *cluster_infos =
    "cluster_state:ok\r\n"
    "cluster_slots_assigned:" + std::to_string(ok_slot) + "\r\n"
    "cluster_slots_ok:" + std::to_string(ok_slot) + "\r\n"
    "cluster_slots_pfail:0\r\n"
    "cluster_slots_fail:0\r\n"
    "cluster_known_nodes:" + std::to_string(nodes_.size()) +"\r\n"
    "cluster_size:" + std::to_string(size_) + "\r\n"
    "cluster_current_epoch:" +  std::to_string(version_) + "\r\n"
    "cluster_my_epoch:" +  std::to_string(version_) + "\r\n";

  return Status::OK();
}

// Format: 1) 1) start slot
//            2) end slot
//            3) 1) master IP
//               2) master port
//               3) node ID
//            4) 1) replica IP
//               2) replica port
//               3) node ID
//          ... continued until done
Status Cluster::GetSlotsInfo(std::vector<SlotInfo> *slots_infos) {
  if (version_ < 0) {
    return Status(Status::ClusterDown,
      "CLUSTERDOWN The cluster is not initialized");
  }
  slots_infos->clear();

  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      slots_infos->emplace_back(GenSlotNodeInfo(start, i-1, n));
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }
  return Status::OK();
}

SlotInfo Cluster::GenSlotNodeInfo(int start, int end, std::shared_ptr<ClusterNode> n) {
  std::vector<SlotInfo::NodeInfo> vn;
  vn.push_back({n->host_, n->port_, n->id_});  // Itslef

  for (const auto &id : n->replicas) {         // replicas
    if (nodes_.find(id) == nodes_.end()) continue;
    vn.push_back({nodes_[id]->host_, nodes_[id]->port_, nodes_[id]->id_});
  }
  return {start, end, vn};
}

// $node $host:$port@$cport $role $master_id/$- $ping_sent $ping_recieved
// $version $connected $slot_range
Status Cluster::GetClusterNodes(std::string *nodes_str) {
  if (version_ < 0) {
    return Status(Status::ClusterDown,
      "CLUSTERDOWN The cluster is not initialized");
  }

  *nodes_str = GenNodesDescription();
  return Status::OK();
}

std::string Cluster::GenNodesDescription() {
  // Generate slots info firstly
  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      if (start == i-1) {
        n->slots_info_ += std::to_string(start) + " ";
      } else {
        n->slots_info_ += std::to_string(start)+ "-" + std::to_string(i-1) + " ";
      }
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  std::string nodes_desc;
  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> n = item.second;

    std::string node_str;
    // ID, host, port
    node_str.append(n->id_ + " ");
    node_str.append(n->host_ + ":" + std::to_string(n->port_) +
        "@" + std::to_string(n->port_+kClusterPortIncr) + " ");

    // Flags
    if (n->id_ == myid_) node_str.append("myself,");
    if (n->role_ == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + n->master_id_ + " ");
    }

    // Ping sent, pong received, config epoch, link status
    node_str.append(std::to_string(std::time(nullptr)*1000-1) + " " +
      std::to_string(std::time(nullptr)*1000) + " " +
      std::to_string(version_) + " " + "connected");

    // Slots
    if (n->slots_info_.size() > 0) n->slots_info_.pop_back();  // Trim space
    if (n->role_ == kClusterMaster) node_str.append(" " + n->slots_info_);
    n->slots_info_.clear();  // Reset

    nodes_desc.append(node_str + "\n");
  }
  return nodes_desc;
}

Status Cluster::ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes) {
  std::vector<std::string> nodes_info;
  Util::Split(nodes_str, "\n", &nodes_info);
  if (nodes_info.size() == 0) {
    return Status(Status::ClusterInvalidInfo, "Invalid cluster nodes info");
  }
  nodes->clear();

  // Parse all nodes
  for (const auto& node_str : nodes_info) {
    std::vector<std::string> fields;
    Util::Split(node_str, " ", &fields);
    if (fields.size() < 5) {
      return Status(Status::ClusterInvalidInfo, "Invalid cluster nodes info");
    }

    // 1) node id
    if (fields[0].size() != kClusetNodeIdLen) {
      return Status(Status::ClusterInvalidInfo, "Invalid cluster node id");
    }
    std::string id = fields[0];

    // 2) host, TODO(@shooterit): check host is valid
    std::string host = fields[1];

    // 3) port
    int port = std::atoi(fields[2].c_str());
    if (port <= 0 || port >= (65535-kClusterPortIncr)) {
      return Status(Status::ClusterInvalidInfo, "Invalid cluste node port");
    }

    // 4) role
    int role;
    if (strcasecmp(fields[3].c_str(), "master") == 0) {
      role = kClusterMaster;
    } else if (strcasecmp(fields[3].c_str(), "slave") == 0 ||
               strcasecmp(fields[3].c_str(), "replica") == 0) {
      role = kClusterSlave;
    } else {
      return Status(Status::ClusterInvalidInfo, "Invalid cluste node role");
    }

    // 5) master id
    std::string master_id = fields[4];
    if ((role == kClusterMaster && master_id != "-") ||
        (role == kClusterSlave && master_id.size() != kClusetNodeIdLen)) {
      return Status(Status::ClusterInvalidInfo, "Invalid cluste node master id");
    }

    std::bitset<kClusterSlots> slots;
    if (role == kClusterSlave) {
      if (fields.size() != 5) {
        return Status(Status::ClusterInvalidInfo, "Invalid cluster nodes info");
      } else {
        // Create slave node
        (*nodes)[id] = std::shared_ptr<ClusterNode>(
          new ClusterNode(id, host, port, role, master_id, slots));
        continue;
      }
    }

    // 6) slot info
    for (unsigned i = 5; i < fields.size(); i++) {
      int start, stop;
      std::vector<std::string> ranges;
      Util::Split(fields[i], "-", &ranges);
      if (ranges.size() == 1) {
        start = std::atoi(ranges[0].c_str());
        if (IsValidSlot(start) == false) {
          return Status(Status::ClusterInvalidInfo, "Invalid cluste slot range");
        }
        slots.set(start, 1);
        if (role == kClusterMaster) {
          if (slots_nodes->find(start) != slots_nodes->end()) {
            return Status(Status::ClusterInvalidInfo, "Slot distribution is overlapped");
          } else {
            (*slots_nodes)[start] = id;
          }
        }
      } else if (ranges.size() == 2) {
        start = std::atoi(ranges[0].c_str());
        stop = std::atoi(ranges[1].c_str());
        if (start >= stop || start < 0 || stop >= kClusterSlots) {
          return Status(Status::ClusterInvalidInfo, "Invalid cluste slot range");
        }
        for (int j = start; j <= stop; j++) {
          slots.set(j, 1);
          if (role == kClusterMaster) {
            if (slots_nodes->find(j) != slots_nodes->end()) {
              return Status(Status::ClusterInvalidInfo, "Slot distribution is overlapped");
            } else {
              (*slots_nodes)[j] = id;
            }
          }
        }
      } else {
        return Status(Status::ClusterInvalidInfo, "Invalid cluste slot range");
      }
    }

    // Create master node
    (*nodes)[id] = std::shared_ptr<ClusterNode>(
        new ClusterNode(id, host, port, role, master_id, slots));
  }
  return Status::OK();
}

Status Cluster::CanExecByMySelf(const Redis::CommandAttributes *attributes,
                         const std::vector<std::string> &cmd_tokens) {
  std::vector<int> keys_indexes;
  auto s = Redis::GetKeysFromCommand(attributes->name, cmd_tokens.size(), &keys_indexes);
  // No keys
  if (!s.IsOK()) return Status::OK();
  if (keys_indexes.size() == 0) return Status::OK();

  int slot = -1;
  for (auto i : keys_indexes) {
    if (i >= static_cast<int>(cmd_tokens.size())) break;
    int cur_slot = GetSlotNumFromKey(cmd_tokens[i]);
    if (slot == -1) slot = cur_slot;
    if (slot != cur_slot) {
      return Status(Status::RedisExecErr,
        "CROSSSLOT Keys in request don't hash to the same slot");
    }
  }
  if (slot == -1) return Status::OK();

  if (slots_nodes_[slot] == nullptr) {
    return Status(Status::ClusterDown, "CLUSTERDOWN Hash slot not served");
  } else if (myself_ && myself_ == slots_nodes_[slot]) {
    return Status::OK();  // I serve this slot
  } else if (myself_ && myself_->role_ == kClusterSlave
     && attributes->is_write() == false
     && nodes_.find(myself_->master_id_) != nodes_.end()
     && nodes_[myself_->master_id_] == slots_nodes_[slot]) {
    return Status::OK();  // My mater serve this slot
  } else {
    std::string ip_port = slots_nodes_[slot]->host_ + ":" +
                          std::to_string(slots_nodes_[slot]->port_);
    return Status(Status::RedisExecErr, "MOVED "+std::to_string(slot)+ " " + ip_port);
  }
}
