#pragma once
#include <string>

// crc16
#define HASH_SLOTS_MASK 0x3fff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)      // 16384
#define HASH_SLOTS_MAX_ITERATIONS 50

uint16_t crc16(const char *buf, int len);
uint16_t GetSlotNumFromKey(const std::string &key);
std::string GetTagFromKey(const std::string &key);
