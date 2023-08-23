#include "hlsh_utils.h"

namespace HLSH_hashing{
using FLAG_VERSION = uint8_t;
constexpr uint8_t FLAG_BITS = 1;
constexpr uint8_t VERSION_BITS = sizeof(FLAG_VERSION) * 8 - FLAG_BITS;
enum FLAG_t { INVALID = 0, VALID = 1 };
const size_t MAX_VALUE_LEN = 512;
const size_t MAX_KEY_LEN = 512;

#pragma pack(1)
template <typename KEY, typename VALUE>
class Pair_t {
 public:
  FLAG_VERSION flag : FLAG_BITS;
  FLAG_VERSION version : VERSION_BITS;
  KEY _key;
  VALUE _value;

  Pair_t() {
    _key = 0;
    _value = 0;
  };

  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
    *this = *pt;
  }
  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
    *this = *pt;
  }
  KEY *key() { return &_key; }
  KEY str_key() { return _key; }
  VALUE value() { return _value; }
  size_t klen() { return sizeof(KEY); }
  Pair_t(KEY k, VALUE v) {
    _key = k;
    _value = v;
  }
  void set_key(KEY k) { _key = k; }

  void store_persist(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(VALUE);
    memcpy(addr + sizeof(FLAG_VERSION), p + sizeof(FLAG_VERSION), len);
    _mm_sfence();
    memcpy(addr, p, sizeof(FLAG_VERSION));
    clwb_sfence(addr, size());
  }

  void store_persist_update(char *addr) { store_persist(addr); }

  void store(void *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = size();
    memcpy(addr, p, len);
  }

  void set_empty() {
    _key = 0;
    _value = 0;
  }

  void set_version(uint64_t old_version) { version = old_version + 1; }

  void set_flag(FLAG_t f) { flag = static_cast<uint8_t>(f); }
  FLAG_t get_flag() { return static_cast<FLAG_t>(flag); }
  void set_flag_persist(FLAG_t f) {
    flag = static_cast<uint8_t>(f);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }

  size_t size() {
    auto total_length = sizeof(FLAG_VERSION) + sizeof(KEY) + sizeof(VALUE);
    return total_length;
  }
};

template <typename KEY>
class Pair_t<KEY, std::string> {
 public:
  FLAG_VERSION flag : FLAG_BITS;
  FLAG_VERSION version : VERSION_BITS;
  KEY _key;
  uint32_t _vlen;
  std::string svalue;
  Pair_t() {
    flag = 0;
    version = 0;
    _vlen = 0;
    svalue.reserve(MAX_VALUE_LEN);
  };
  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    _key = pt->_key;
    flag = pt->flag;
    version = pt->version;
    _vlen = pt->_vlen;
    svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _vlen);
  }

  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    _key = pt->_key;
    flag = pt->flag;
    version = pt->version;
    _vlen = pt->_vlen;
    svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _vlen);
  }

  size_t klen() { return sizeof(KEY); }
  KEY *key() { return &_key; }
  KEY str_key() { return _key; }
  std::string str_value() { return svalue; }

  Pair_t(KEY k, char *v_ptr, size_t vlen) : _vlen(vlen), version(0) {
    _key = k;
    svalue.assign(v_ptr, _vlen);
  }

  void set_key(KEY k) { _key = k; }

  void store_persist(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t);
    memcpy(addr + sizeof(FLAG_VERSION), p + sizeof(FLAG_VERSION), len);
    memcpy(addr + len, &svalue[0], _vlen);
    _mm_sfence();
    memcpy(addr, p, sizeof(FLAG_VERSION));
    clwb_sfence(addr, size());
  }

  void store_persist_update(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &svalue[0], _vlen);
    reinterpret_cast<Pair_t<KEY, std::string> *>(addr)->set_version(version);
    pmem_persist(addr, len + _vlen);
  }

  void store(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &svalue[0], _vlen);
  }

  void set_empty() { _vlen = 0; }
  void set_version(uint64_t old_version) { version = old_version + 1; }
  void set_flag(FLAG_t f) { flag = static_cast<uint8_t>(f); }
  FLAG_t get_flag() { return flag; }
  void set_flag_persist(FLAG_t f) {
    flag = static_cast<uint8_t>(f);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }
  size_t size() {
    auto total_length =
        sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION) + _vlen;
    return total_length;
  }
};

template <>
class Pair_t<std::string, std::string> {
 public:
  FLAG_VERSION flag : FLAG_BITS;
  FLAG_VERSION version : VERSION_BITS;
  uint32_t _klen;
  uint32_t _vlen;
  std::string skey;
  std::string svalue;
  Pair_t() {
    flag = 0;
    version = 0;
    _klen = 0;
    _vlen = 0;
    skey.reserve(MAX_KEY_LEN);
    svalue.reserve(MAX_VALUE_LEN);
  };
  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    flag = pt->flag;
    version = pt->version;
    _klen = pt->_klen;
    _vlen = pt->_vlen;
    skey.assign(p + 6, _klen);
    svalue.assign(p + 6 + _klen, _vlen);
  }
  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    flag = pt->flag;
    version = pt->version;
    _klen = pt->_klen;
    _vlen = pt->_vlen;
    skey.assign(p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _klen);
    svalue.assign( p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION) + _klen, _vlen);
  }
  size_t klen() { return _klen; }
  char *key() { return &skey[0]; }
  std::string str_key() { return skey; }
  std::string value() { return svalue; }

  Pair_t(char *k_ptr, size_t klen, char *v_ptr, size_t vlen)
      : _klen(klen), _vlen(vlen), version(0) {
    skey.assign(k_ptr, _klen);
    svalue.assign(v_ptr, _vlen);
  }
  void set_key(char *k_ptr, size_t kl) {
    _klen = kl;
    skey.assign(k_ptr, _klen);
  }

  void store_persist(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t);
    memcpy(addr + sizeof(FLAG_VERSION), p, len);
    memcpy(addr + sizeof(FLAG_VERSION) + len, &skey[0], _klen);
    memcpy(addr + sizeof(FLAG_VERSION) + len + _klen, &svalue[0], _vlen);
    _mm_sfence();
    memcpy(addr, p, sizeof(FLAG_VERSION));
    clwb_sfence(addr, size());
  }

  void store_persist_update(char *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &skey[0], _klen);
    memcpy(addr + len + _klen, &svalue[0], _vlen);
    reinterpret_cast<Pair_t<std::string, std::string> *>(addr)->set_version(
        version);
    pmem_persist(addr, len + _klen + _vlen);
  }
  void store(char *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &skey[0], _klen);
    memcpy(addr + len + _klen, &svalue[0], _vlen);
  }
  void set_empty() {
    _klen = 0;
    _vlen = 0;
  }
  void set_version(uint64_t old_version) { version = old_version + 1; }
  void set_flag(FLAG_t f) { flag = static_cast<uint16_t>(f); }
  FLAG_t get_flag() { return static_cast<FLAG_t>(flag); }
  void set_flag_persist(FLAG_t f) {
    flag = static_cast<uint16_t>(f);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }

  size_t size() {
    auto total_length = sizeof(uint32_t) + sizeof(uint32_t) +
                        sizeof(FLAG_VERSION) + _klen + _vlen;
    return total_length;
  }
};
#pragma pack()
}