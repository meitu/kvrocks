#include <gtest/gtest.h>
#include "semisync_master.h"

class SemiSyncMasterTest : public testing::Test {
 protected:
  ~SemiSyncMasterTest() {
  }
  static void SetUpTestCase() {
  }
  static void TearDownTestCase() {
  }

  virtual void SetUp() {
    node_manager = new WaitingNodeManager();
    container = new AckContainer();
  }

  virtual void TearDown() {
    delete node_manager;
    delete container;
  }

  WaitingNodeManager* node_manager = nullptr;
  AckContainer* container = nullptr;
};

TEST_F(SemiSyncMasterTest, WaitingNodeManager) {
  uint64_t pos1 = 1000;
  uint64_t pos2 = 2000;
  uint64_t pos3 = 1700;
  uint64_t pos4 = 1400; 

  node_manager->InsertWaitingNode(pos1);
  auto* waiting_node = node_manager->FindWaitingNode(pos1);
  ASSERT_TRUE(waiting_node != nullptr);
  EXPECT_EQ(waiting_node->log_pos, pos1);

  node_manager->InsertWaitingNode(pos2);
  node_manager->InsertWaitingNode(pos3);
  node_manager->InsertWaitingNode(pos4);
  int ret_num = node_manager->SignalWaitingNodesUpTo(1500);
  EXPECT_EQ(ret_num, 2);

  ret_num = node_manager->SignalWaitingNodesAll();
  EXPECT_EQ(ret_num, 4);

  node_manager->ClearWaitingNodes(1500);
  auto* r1 = node_manager->FindWaitingNode(pos1);
  EXPECT_TRUE(r1 != nullptr);
  EXPECT_EQ(r1->log_pos, pos3);
  auto* r2 = node_manager->FindWaitingNode(pos2);
  EXPECT_TRUE(r2 != nullptr);
  EXPECT_EQ(r2->log_pos, pos2);
  auto* r3 = node_manager->FindWaitingNode(pos3);
  EXPECT_TRUE(r3 != nullptr);
  EXPECT_EQ(r3->log_pos, pos3);
  auto* r4 = node_manager->FindWaitingNode(pos4);
  EXPECT_TRUE(r4 != nullptr);
  EXPECT_EQ(r4->log_pos, pos3);
}

TEST_F(SemiSyncMasterTest, AckContainer) {
  const AckInfo *ackinfo = nullptr;
  auto r = container->Resize(3, &ackinfo);

  int server_1 = 1; uint64_t seq_1 = 1000;
  int server_2 = 2; uint64_t seq_2 = 999;
  int server_3 = 3; uint64_t seq_3 = 1201;
  int server_4 = 4; uint64_t seq_4 = 1202;
  int server_5 = 5; uint64_t seq_5 = 1203;
  
  container->Insert(server_1, seq_1);
  container->Insert(server_2, seq_2);
  r = container->Resize(1, &ackinfo);
  ASSERT_EQ(r, true);
  EXPECT_EQ(ackinfo->log_pos, seq_1);

  ackinfo = nullptr;
  container->Resize(3, &ackinfo);
  ASSERT_EQ(ackinfo, nullptr);

  container->Insert(server_3, seq_3);
  container->Insert(server_4, seq_4);
  auto insert_value = container->Insert(server_5, seq_5);
  ASSERT_TRUE(insert_value != nullptr);
  EXPECT_EQ(insert_value->log_pos, seq_3);
}