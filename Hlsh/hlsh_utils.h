#include <immintrin.h>
#include <libpmem.h>
#include <omp.h>
#include <sys/stat.h>

#include <bitset>
#include <cassert>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <unistd.h>

namespace HLSH_hashing {
#define ALIGNED(N) __attribute__((aligned(N)))
#define PACKED __attribute__((packed))
#define CHECK_BIT(var, pos) ((var) & (1 << (pos)))
#define CHECK_BITL(var, pos) ((var) & (1UL << (pos)))
#define SET_BITL(var, pos) ((var) | (1UL << (pos)))
#define UNSET_BITL(var, pos) ((var) & (~(1UL << (pos))))
#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)
#define MASKL(m, p) ((m) & ((1UL << (p)) - 1))

  // bind thread and cpu core
  void set_affinity(uint32_t idx)
  {
    cpu_set_t my_set;
    CPU_ZERO(&my_set);
    if (idx < 18)
    {
      CPU_SET(idx, &my_set);
    } else {
    CPU_SET(idx + 18, &my_set);
  }
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
}

static inline size_t h(const void* key, size_t len, size_t seed = 0xc70697UL) {
  return std::_Hash_bytes(key, len, seed);
}

static constexpr const uint32_t kCacheLineSize = 64;

inline void clwb_sfence(const void* addr, const size_t len) {
  char* addr_ptr = (char*)addr;
  char* end_ptr = addr_ptr + len;
  for (; addr_ptr < end_ptr; addr_ptr += kCacheLineSize) {
    _mm_clwb(addr_ptr);
  }
  _mm_sfence();
}

inline void stream512_sfence(void *addr, const size_t len)
{
  // s: flush 64-byte unaligned
  auto a = reinterpret_cast<uint64_t>(addr);
  auto unligned_len = a & 63;
  if (unligned_len)
  {
    _mm_clwb((char *)addr);
  }
  auto b = a + (64 - unligned_len);
  // s: flush 64-byte aligned
  auto start = (char *)(b);
  auto end = start + len - (64 - unligned_len);
  bool need_clwb = false;
  for (; start < end; start = start + 64)
  {
    if ((start + 64) < end)
    {
      need_clwb = true;
      break;
    }
    auto data = reinterpret_cast<__m512i *>(start);
    _mm512_stream_si512(data, *data);
  }
  // s: flush 64-byte unaligned 
  if (need_clwb)
  {
    for (auto i = start; i < end; i = i + 64)
    {
      _mm_clwb(i);
    }
  }
  _mm_sfence();
}

inline void clushopt_sfence(const void* addr, const size_t len) {
  char* addr_ptr = (char*)addr;
  char* end_ptr = addr_ptr + len;
  for (; addr_ptr < end_ptr; addr_ptr += kCacheLineSize) {
    _mm_clflushopt(addr_ptr);
  }
  _mm_sfence();
}

inline void memcpy_persist(void *dest, const void *src, const size_t len)
{
  memcpy(dest, src, len);
  clwb_sfence(dest, len);
}

inline void memcpy_persist512(void *dest, const void *src, const size_t len)
{
  memcpy(dest, src, len);
  stream512_sfence(dest, len);
}

static bool FileExists(const char* pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

static bool FileRemove(const char* pool_path) {
  struct stat buffer;
  if (stat(pool_path, &buffer) == 0) {
    if (buffer.st_mode & S_IFDIR) {
      std::filesystem::remove_all(pool_path);
      printf("succeed to remove pool file directory\n");
    } else if (buffer.st_mode & S_IFREG) {
      remove(pool_path);
      printf("succeed to remove pool file\n");
    }
  }
  return true;
}

#define CAS(_p, _u, _v)                                             \
  (__atomic_compare_exchange_n((_p), (_u), (_v), false, __ATOMIC_RELEASE, \
                               __ATOMIC_ACQUIRE))
#define LOAD(_p) (__atomic_load_n((_p), __ATOMIC_ACQUIRE))
#define STORE(_p, _v) (__atomic_store_n((_p), (_v), __ATOMIC_RELEASE))
// ADD and SUB return the value after add or sub
#define ADD(_p, _v) (__atomic_add_fetch((_p), (_v), __ATOMIC_SEQ_CST))
#define SUB(_p, _v) (__atomic_sub_fetch((_p), (_v), __ATOMIC_SEQ_CST))

  inline uint8_t CMP64(uint8_t src[], uint8_t key)
  {
    const __m64 key_data = _mm_set1_pi8(key);
    __m64 seg_data = _mm_set_pi8(src[7], src[6], src[5], src[4], src[3], src[2], src[1], src[0]);
    __m64 rv_mask = _mm_cmpeq_pi8(seg_data, key_data);
    return _mm_movemask_pi8(rv_mask);
  }

#define MMX_CMP8(src, key)                                               \
  do {                                                                   \
    const __m64 key_data =  _mm_set1_pi8(key);            \
    __m64 seg_data = _mm_set_pi8(src[7], src[6], src[5], src[4], src[3], \
                                 src[2], src[1], src[0]);                \
    __m64 rv_mask = _mm_cmpeq_pi8(seg_data, key_data);                   \
    mask = _mm_movemask_pi8(rv_mask);                                    \
  } while (0)

// 128 bytes
inline uint16_t CMP128(uint8_t src[], uint8_t key)
{
  const __m128i key_data = _mm_set1_epi8(key);
  __m128i seg_data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));
  return _mm_cmpeq_epi8_mask(seg_data, key_data);
}

#define SSE_CMP8(src, key)                                                     \
  do{                                                                         \
    const __m128i key_data = _mm_set1_epi8(key);                               \
    __m128i seg_data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src)); \
    __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);                      \
    mask = _mm_movemask_epi8(rv_mask);                                         \
  }while(0)

    // mask = _mm_cmpeq_epi8_mask(seg_data,key_data);\

// 256 bytes
#define SIMD_CMP8(src, key)                                        \
  do {                                                             \
    const __m256i key_data = _mm256_set1_epi8(key);                \
    __m256i seg_data =                                             \
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)); \
    __m256i rv_mask = _mm256_cmpeq_epi8(seg_data, key_data);       \
    mask = _mm256_movemask_epi8(rv_mask);                          \
  } while (0)

// 512 bytes
#define AVX_CMP8(src, key)                            \
  do {                                                \
    __m512i vec1 = _mm512_loadu_si512((__m512i*)src); \
    __m512i vec2 = _mm512_set1_epi8(key);             \
    mask = _mm512_cmpeq_epi8_mask(vec1, vec2);        \
  } while (0)

};  // namespace HLSH_hashing