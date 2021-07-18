// Copyright Bing-a-ling (https://github.com/Bing-a-ling/ReadWriteLock)
// Copyright Kvrocks Core Team (bugfix and adapted to kvrocks coding style)
// All rights reserved.
//
// A implementation of RAII write-first read-write lock using C++11.

#pragma once
#include <mutex>
#include <condition_variable>

namespace RWLock {

class ReadWriteLock {
 public:
  ReadWriteLock() : read_count_(0), write_count_(0), is_writing_(false) {}
  virtual ~ReadWriteLock() = default;

  void LockWrite() {
    std::unique_lock<std::mutex> guard(lock_);
    ++write_count_;
    write_condition_.wait(guard, [=] {
        return read_count_ == 0 && !is_writing_;
    });
    is_writing_ = true;
  }

  void UnLockWrite() {
    std::lock_guard<std::mutex> guard(lock_);
    is_writing_ = false;
    if (--write_count_ == 0) {
        read_condition_.notify_all();
    } else {
        write_condition_.notify_one();
    }
  }

  void LockRead() {
    std::unique_lock<std::mutex> guard(lock_);
    read_condition_.wait(guard, [=] {
        return write_count_ == 0;
    });
    ++read_count_;
  }

  void UnLockRead() {
    std::lock_guard<std::mutex> guard(lock_);
    if (--read_count_ == 0 && write_count_ > 0) {
        write_condition_.notify_one();
    }
  }

 private:
  int read_count_;
  int write_count_;
  bool is_writing_;
  std::mutex lock_;
  std::condition_variable read_condition_;
  std::condition_variable write_condition_;
};

class WriteLock {
 public:
  explicit WriteLock(ReadWriteLock& wlock) : wlock_(wlock) {
    wlock_.LockWrite();
  }

  virtual ~WriteLock() {
    wlock_.UnLockWrite();
  }

 private:
  ReadWriteLock& wlock_;
};

class ReadLock {
 public:
  explicit ReadLock(ReadWriteLock& rlock) : rlock_(rlock) {
    rlock_.LockRead();
  }

  virtual ~ReadLock() {
    rlock_.UnLockRead();
  }

 private:
  ReadWriteLock& rlock_;
};

}  // namespace RWLock
