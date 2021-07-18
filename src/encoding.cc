#include "encoding.h"

#include <limits.h>
#include <float.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
/* Byte ordering detection */
#include <sys/types.h> /* This will likely define BYTE_ORDER */

#include <string>
#include <utility>

#ifndef BYTE_ORDER
#if (BSD >= 199103)
# include <machine/endian.h>
#else
#if defined(linux) || defined(__linux__)
# include <endian.h>
#else
#define LITTLE_ENDIAN   1234    /* least-significant byte first (vax, pc) */
#define BIG_ENDIAN  4321    /* most-significant byte first (IBM, net) */
#define PDP_ENDIAN  3412    /* LSB first in word, MSW first in long (pdp)*/

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || \
  defined(vax) || defined(ns32000) || defined(sun386) || \
  defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || \
  defined(__alpha__) || defined(__alpha)
#define BYTE_ORDER    LITTLE_ENDIAN
#endif

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || \
  defined(vax) || defined(ns32000) || defined(sun386) || \
  defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || \
  defined(__alpha__) || defined(__alpha)
#define BYTE_ORDER    LITTLE_ENDIAN
#endif

#if defined(sel) || defined(pyr) || defined(mc68000) || defined(sparc) || \
  defined(is68k) || defined(tahoe) || defined(ibm032) || defined(ibm370) || \
  defined(MIPSEB) || defined(_MIPSEB) || defined(_IBMR2) || defined(DGUX) ||\
  defined(apollo) || defined(__convex__) || defined(_CRAY) || \
  defined(__hppa) || defined(__hp9000) || \
  defined(__hp9000s300) || defined(__hp9000s700) || \
  defined(BIT_ZERO_ON_LEFT) || defined(m68k) || defined(__sparc)
#define BYTE_ORDER  BIG_ENDIAN
#endif
#endif /* linux */
#endif /* BSD */
#endif /* BYTE_ORDER */

/* Sometimes after including an OS-specific header that defines the
 * endianess we end with __BYTE_ORDER but not with BYTE_ORDER that is what
 * the Redis code uses. In this case let's define everything without the
 * underscores. */
#ifndef BYTE_ORDER
#ifdef __BYTE_ORDER
#if defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif
#endif
#endif

#if !defined(BYTE_ORDER) || \
    (BYTE_ORDER != BIG_ENDIAN && BYTE_ORDER != LITTLE_ENDIAN)
/* you must determine what the correct bit order is for
     * your compiler - the next line is an intentional error
     * which will force your compiles to bomb until you fix
     * the above macros.
     */
#error "Undefined or invalid BYTE_ORDER"
#endif

void EncodeFixed8(char *buf, uint8_t value) {
  buf[0] = static_cast<uint8_t>(value & 0xff);
}

void EncodeFixed16(char *buf, uint16_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<uint8_t>((value >> 8) & 0xff);
    buf[1] = static_cast<uint8_t>(value & 0xff);
  }
}

void EncodeFixed32(char *buf, uint32_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<uint8_t>((value >> 24) & 0xff);
    buf[1] = static_cast<uint8_t>((value >> 16) & 0xff);
    buf[2] = static_cast<uint8_t>((value >> 8) & 0xff);
    buf[3] = static_cast<uint8_t>(value & 0xff);
  }
}

void EncodeFixed64(char *buf, uint64_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<uint8_t>((value >> 56) & 0xff);
    buf[1] = static_cast<uint8_t>((value >> 48) & 0xff);
    buf[2] = static_cast<uint8_t>((value >> 40) & 0xff);
    buf[3] = static_cast<uint8_t>((value >> 32) & 0xff);
    buf[4] = static_cast<uint8_t>((value >> 24) & 0xff);
    buf[5] = static_cast<uint8_t>((value >> 16) & 0xff);
    buf[6] = static_cast<uint8_t>((value >> 8) & 0xff);
    buf[7] = static_cast<uint8_t>(value & 0xff);
  }
}

void PutFixed8(std::string *dst, uint8_t value) {
  char buf[1];
  buf[0] = static_cast<uint8_t>(value & 0xff);
  dst->append(buf, 1);
}

void PutFixed16(std::string *dst, uint16_t value) {
  char buf[sizeof(value)];
  EncodeFixed16(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed32(std::string *dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string *dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutDouble(std::string *dst, double value) {
  uint64_t u64;
  memcpy(&u64, &value, sizeof(value));
  auto ptr = &u64;
  if ((*ptr >> 63) == 1) {
    // signed bit would be zero
    *ptr ^= 0xffffffffffffffff;
  } else {
    // signed bit would be one
    *ptr |= 0x8000000000000000;
  }
  PutFixed64(dst, *ptr);
}

bool GetFixed8(rocksdb::Slice *input, uint8_t *value) {
  const char *data;
  if (input->size() < sizeof(uint8_t)) {
    return false;
  }
  data = input->data();
  *value = static_cast<uint8_t>(data[0] & 0xff);
  input->remove_prefix(sizeof(uint8_t));
  return true;
}

bool GetFixed64(rocksdb::Slice *input, uint64_t *value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->remove_prefix(sizeof(uint64_t));
  return true;
}

bool GetFixed32(rocksdb::Slice *input, uint32_t *value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->remove_prefix(sizeof(uint32_t));
  return true;
}

bool GetFixed16(rocksdb::Slice *input, uint16_t *value) {
  if (input->size() < sizeof(uint16_t)) {
    return false;
  }
  *value = DecodeFixed16(input->data());
  input->remove_prefix(sizeof(uint16_t));
  return true;
}

bool GetDouble(rocksdb::Slice *input, double *value) {
  if (input->size() < sizeof(double)) return false;
  *value = DecodeDouble(input->data());
  input->remove_prefix(sizeof(double));
  return true;
}

uint16_t DecodeFixed16(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint16_t value;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    return ((static_cast<uint16_t>(static_cast<uint8_t>(ptr[1])))
        | (static_cast<uint16_t>(static_cast<uint8_t>(ptr[0])) << 8));
  }
}

uint32_t DecodeFixed32(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint32_t value;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    return ((static_cast<uint32_t>(static_cast<uint8_t>(ptr[3])))
        | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[2])) << 8)
        | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[1])) << 16)
        | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[0])) << 24));
  }
}

uint64_t DecodeFixed64(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint64_t value;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    uint64_t hi = DecodeFixed32(ptr);
    uint64_t lo = DecodeFixed32(ptr+4);
    return (hi << 32) | lo;
  }
}

double DecodeDouble(const char *ptr) {
  uint64_t decoded = DecodeFixed64(ptr);
  if ((decoded>>63) == 0) {
    decoded ^= 0xffffffffffffffff;
  } else {
    decoded &= 0x7fffffffffffffff;
  }
  double value;
  memcpy(&value, &decoded, sizeof(value));
  return value;
}
