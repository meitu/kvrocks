#include "test_base.h"
#include "redis_list.h"
#include <gtest/gtest.h>

class RedisListTest : public TestBase {
protected:
  explicit RedisListTest():TestBase() {
    list = new Redis::List(storage_, "list_ns");
  }
  ~RedisListTest() {
    delete list;
  }
  void SetUp() override {
    key_ = "test-list-key";
    fields_ = {
                "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5"
    };
  }

protected:
  Redis::List *list;
};

class RedisListSpecificTest : public RedisListTest {
protected:
  void SetUp() override {
    key_ = "test-list-specific-key";
    fields_ = {"0", "1", "2", "3", "4", "3", "6", "7", "3", "8", "9", "3", "9", "3", "9"};
  }
};

TEST_F(RedisListTest, PushAndPop) {
  int ret;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, false);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Pushx) {
  int ret;
  Slice pushx_key("test-pushx-key");
  rocksdb::Status s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_TRUE(s.ok());
  list->Push(pushx_key, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_EQ(ret, fields_.size()*2);
  list->Del(pushx_key);
}

TEST_F(RedisListTest, Index) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Index(key_,i, &elem);
    EXPECT_EQ(fields_[i].ToString(), elem);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  rocksdb::Status s = list->Index(key_,-1, &elem);
  EXPECT_TRUE(s.IsNotFound());
  list->Del(key_);
}

TEST_F(RedisListTest, Set) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice new_elem("new_elem");
  list->Set(key_, -1, new_elem);
  std::string elem;
  list->Index(key_, -1, &elem);
  EXPECT_EQ(new_elem.ToString(), elem);
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Pop(key_, &elem, true);
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Range) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  list->Range(key_, 0, int(elems.size()-1), &elems);
  EXPECT_EQ(elems.size(), fields_.size());
  for (size_t i = 0; i < elems.size(); i++) {
    EXPECT_EQ(fields_[i].ToString(), elems[i]);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Rem) {
  int ret;
  uint32_t len;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice del_elem("list-test-key-1");
  // lrem key_ 1 list-test-key-1
  list->Rem(key_, 1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-1, len);
  for (size_t i = 1; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ 0 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, 0, del_elem, &ret);
  EXPECT_EQ(4, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-4, len);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    if (fields_[i] == del_elem) continue;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ 1 nosuchelement
  Slice no_elem("no_such_element");
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, 1, no_elem, &ret);
  EXPECT_EQ(0, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size(), len);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ -1 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  list->Rem(key_, -1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-1, len);
  int cnt = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    if (fields_[i] == del_elem) {
      if (++cnt > 3) continue;
    }
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ -5 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, -5, del_elem, &ret);
  EXPECT_EQ(4, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-4, len);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    if (fields_[i] == del_elem) continue;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListSpecificTest, Rem) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice del_elem("9");
  // lrem key_ 1 9
  list->Rem(key_, 1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  uint32_t len;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-1, len);
  int cnt = 0;
  for (size_t i = 0; i < fields_.size(); i++) {
    if (fields_[i] == del_elem) {
      if (++cnt <= 1) continue;
    }
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ -2 9
  list->Push(key_, fields_, false, &ret);
  list->Rem(key_, -2, del_elem, &ret);
  EXPECT_EQ(2, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-2, len);
  cnt = 0;
  for (size_t i = fields_.size(); i > 0; i--) {
    if (fields_[i-1] == del_elem) {
      if (++cnt <= 2) continue;
    }
    std::string elem;
    list->Pop(key_, &elem, false);
    EXPECT_EQ(elem, fields_[i-1].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Trim) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Trim(key_, 1, 2000);
  uint32_t len;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-1, len);
  for (size_t i = 1; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListSpecificTest, Trim) {
  int ret;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  // ltrim key_ 3 -3 then linsert 2 3 and lrem key_ 5 3
  Slice del_elem("3");
  list->Trim(key_, 3, -3);
  uint32_t len;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size()-5, len);
  Slice insert_elem("3");
  list->Insert(key_, Slice("2"), insert_elem, true, &ret);
  EXPECT_EQ(-1, ret);
  list->Rem(key_, 5, del_elem, &ret);
  EXPECT_EQ(4, ret);
  for (size_t i = 3; i < fields_.size()-2; i++) {
    if (fields_[i] == del_elem) continue;
    std::string elem;
    list->Pop(key_, &elem, true);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, RPopLPush) {
  int ret;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice dst("test-list-rpoplpush-key");
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->RPopLPush(key_, dst, &elem);
    EXPECT_EQ(fields_[i].ToString(), elem);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(dst, &elem, false);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
  list->Del(dst);
}