#include "lock_manager.h"
#include "rw_lock.h"
#include <thread>
#include <memory>
#include <gtest/gtest.h>

TEST(LockManager, LockKey) {
  LockManager locks(8);
  std::vector<rocksdb::Slice> keys = {"abc", "123", "456", "abc", "123"};
  for (const auto key : keys) {
    locks.Lock(key);
    locks.UnLock(key);
  }
}

TEST(ReadWriteLock, ReadLockGurad) {
  RWLock::ReadWriteLock rwlock;
  int val = 1;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
      RWLock::ReadLock rlock(rwlock);
      ASSERT_EQ(1, val);
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
}

TEST(ReadWriteLock, WriteLockGurad) {
  RWLock::ReadWriteLock rwlock;
  int val = 0;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
        RWLock::WriteLock wlock(rwlock);
        for (int i = 0; i < 100000000; i++) {
          val++;
      }
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
  ASSERT_EQ(1000000000, val);
}

TEST(ReadWriteLock, unique_ptr_for_WriteLockGuard) {
  RWLock::ReadWriteLock rwlock;
  int val = 0;

  std::thread ths[10];
  for(int i = 0; i < 10; i++) {
    ths[i] = std::thread([&rwlock, &val]() {
      auto ptr = std::unique_ptr<RWLock::WriteLock>(new RWLock::WriteLock(rwlock));
      for (int i = 0; i < 10000000; i++) {
        val++;
      }
    });
  }

  for (int i = 0; i < 10; i++) {
    ths[i].join();
  }
  ASSERT_EQ(100000000, val);
}

TEST(ReadWriteLock, ReadLockGuard_Concurrency) {
  RWLock::ReadWriteLock rwlock;

  std::time_t start = std::time(nullptr);
  std::thread ths[5];
  for(int i = 0; i < 5; i++) {
    ths[i] = std::thread([&rwlock]() {
      auto ptr = std::unique_ptr<RWLock::ReadLock>(new RWLock::ReadLock(rwlock));
      sleep(1);
    });
  }

  for (int i = 0; i < 5; i++) {
    ths[i].join();
  }

  std::time_t end = std::time(nullptr);
  ASSERT_LE(end-start, 2);
}

TEST(ReadWriteLock, WriteLockGuard_Exclusive) {
  RWLock::ReadWriteLock rwlock;

  std::time_t start = std::time(nullptr);
  std::thread ths[5];
  for(int i = 0; i < 5; i++) {
    ths[i] = std::thread([&rwlock]() {
      auto ptr = std::unique_ptr<RWLock::WriteLock>(new RWLock::WriteLock(rwlock));
      sleep(1);
    });
  }

  for (int i = 0; i < 5; i++) {
    ths[i].join();
  }

  std::time_t end = std::time(nullptr);
  ASSERT_GT(end-start, 4);
}

TEST(ReadWriteLock, WriteLockGurad_First) {
  RWLock::ReadWriteLock rwlock;
  int val = 0;

  std::thread ths[6];
  for(int i = 0; i < 6; i++) {
    if ((i % 2) == 0) {
      ths[i] = std::thread([&rwlock, &val]() {
        auto ptr = std::unique_ptr<RWLock::WriteLock>(new RWLock::WriteLock(rwlock));
        sleep(1);  // The second write lock thread will get right to process
        val++;
      });
    } else {
      ths[i] = std::thread([&rwlock, &val]() {
        usleep(1000);  // To avoid it is the first to run, just sleep 100ms
        auto ptr = std::unique_ptr<RWLock::ReadLock>(new RWLock::ReadLock(rwlock));
        ASSERT_EQ(val, 3);
      });
    }
  }

  for (int i = 0; i < 6; i++) {
    ths[i].join();
  }
}
