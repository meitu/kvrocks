#include "redis_bitmap.h"
#include <vector>

#include "redis_bitmap_string.h"

namespace Redis {

const uint32_t kBitmapSegmentBits = 1024 * 8;
const uint32_t kBitmapSegmentBytes = 1024;

uint32_t kNum2Bits[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

rocksdb::Status Bitmap::GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value) {
  std::string old_metadata;
  metadata->Encode(&old_metadata);
  auto s = GetRawMetadata(ns_key, raw_value);
  if (!s.ok()) return s;
  metadata->Decode(*raw_value);

  if (metadata->Expired()) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("the key was Expired");
  }
  if (metadata->Type() == kRedisString) return s;
  if (metadata->Type() != kRedisBitmap && metadata->size > 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  if (metadata->size == 0) {
    metadata->Decode(old_metadata);
    return rocksdb::Status::NotFound("no elements");
  }
  return s;
}

rocksdb::Status Bitmap::GetBit(const Slice &user_key, uint32_t offset, bool *bit) {
  *bit = false;
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.GetBit(raw_value, offset, bit);
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  uint32_t index = (offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
  std::string sub_key, value;
  InternalKey(ns_key, std::to_string(index), metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  s = db_->Get(read_options, sub_key, &value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  if ((byte_index < value.size() && (value[byte_index] & (1 << (offset % 8))))) {
    *bit = true;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit) {
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  BitmapMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.SetBit(ns_key, &raw_value, offset, new_bit, old_bit);
  }

  std::string sub_key, value;
  uint32_t index = (offset / kBitmapSegmentBits) * kBitmapSegmentBytes;
  InternalKey(ns_key, std::to_string(index), metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
  }
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  uint32_t bitmap_size = metadata.size;
  if (byte_index >= value.size()) {  // expand the bitmap
    size_t expand_size;
    if (byte_index >= value.size() * 2) {
      expand_size = byte_index - value.size() + 1;
    } else {
      expand_size = value.size();
    }
    value.append(expand_size, 0);
    if (value.size() + index > bitmap_size) {
      bitmap_size = static_cast<uint32_t>(value.size()) + index;
    }
  }
  uint32_t bit_offset = offset % 8;
  *old_bit = (value[byte_index] & (1 << bit_offset)) != 0;
  if (new_bit) {
    value[byte_index] |= 1 << bit_offset;
  } else {
    value[byte_index] &= ~(1 << bit_offset);
  }
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisBitmap, {std::to_string(offset)});
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, value);
  if (metadata.size != bitmap_size) {
    metadata.size = bitmap_size;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Bitmap::BitCount(const Slice &user_key, int start, int stop, uint32_t *cnt) {
  *cnt = 0;
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitCount(raw_value, start, stop, cnt);
  }

  if (start < 0) start += metadata.size + 1;
  if (stop < 0) stop += metadata.size + 1;
  if (stop > static_cast<int>(metadata.size)) stop = metadata.size;
  if (start < 0 || stop <= 0 || start >= stop) return rocksdb::Status::OK();

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  int start_index = start / kBitmapSegmentBytes;
  int stop_index = stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  std::string sub_key, value;
  for (int i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version,
                  storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) continue;
    size_t j = 0;
    if (i == start_index) j = start % kBitmapSegmentBytes;
    for (; j < value.size(); j++) {
      if (i == stop_index && j > (stop % kBitmapSegmentBytes)) break;
      *cnt += kNum2Bits[static_cast<uint8_t>(value[j])];
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Bitmap::BitPos(const Slice &user_key, bool bit, int start, int stop, bool stop_given, int *pos) {
  std::string ns_key, raw_value;
  AppendNamespacePrefix(user_key, &ns_key);

  BitmapMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata, &raw_value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *pos = bit ? -1 : 0;
    return rocksdb::Status::OK();
  }

  if (metadata.Type() == kRedisString) {
    Redis::BitmapString bitmap_string_db(storage_, namespace_);
    return bitmap_string_db.BitPos(raw_value, bit, start, stop, stop_given, pos);
  }

  if (start < 0) start += metadata.size + 1;
  if (stop < 0) stop += metadata.size + 1;
  if (start < 0 || stop < 0 || start > stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }

  auto bitPosInByte = [](char byte, bool bit) -> int {
    for (int i = 0; i < 8; i++) {
      if (bit && (byte & (1 << i)) != 0) return i;
      if (!bit && (byte & (1 << i)) == 0) return i;
    }
    return -1;
  };

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  int start_index = start / kBitmapSegmentBytes;
  int stop_index = stop / kBitmapSegmentBytes;
  // Don't use multi get to prevent large range query, and take too much memory
  std::string sub_key, value;
  for (int i = start_index; i <= stop_index; i++) {
    InternalKey(ns_key, std::to_string(i * kBitmapSegmentBytes), metadata.version,
                  storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      if (!bit) {
        *pos = i * kBitmapSegmentBits;
        return rocksdb::Status::OK();
      }
      continue;
    }
    size_t j = 0;
    if (i == start_index) j = start % kBitmapSegmentBytes;
    for (; j < value.size(); j++) {
      if (i == stop_index && j > (stop % kBitmapSegmentBytes)) break;
      if (bitPosInByte(value[j], bit) != -1) {
        *pos = static_cast<int>(i * kBitmapSegmentBits + j * 8 + bitPosInByte(value[j], bit));
        return rocksdb::Status::OK();
      }
    }
    if (!bit && value.size() < kBitmapSegmentBytes) {
      *pos = static_cast<int>(i * kBitmapSegmentBits + value.size() * 8);
      return rocksdb::Status::OK();
    }
  }
  // bit was not found
  *pos = bit ? -1 : static_cast<int>(metadata.size * 8);
  return rocksdb::Status::OK();
}

bool Bitmap::GetBitFromValueAndOffset(const std::string &value, uint32_t offset) {
  bool bit = false;
  uint32_t byte_index = (offset / 8) % kBitmapSegmentBytes;
  if ((byte_index < value.size() && (value[byte_index] & (1 << (offset % 8))))) {
    bit = true;
  }
  return bit;
}

bool Bitmap::IsEmptySegment(const Slice &segment) {
  static const char zero_byte_segment[kBitmapSegmentBytes] = {0};
  std::string value = segment.ToString();
  return !memcmp(zero_byte_segment, value.c_str(), value.size());
}
}  // namespace Redis
