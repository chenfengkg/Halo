#include "hlsh_utils.h"

namespace HLSH_hashing
{
  using FLAG_VERSION = uint8_t;
  constexpr uint8_t FLAG_BITS = 2;
  constexpr uint8_t VERSION_BITS = sizeof(FLAG_VERSION) * 8 - FLAG_BITS;
  enum FLAG_t
  {
    INVALID = 0,
    VALID = 1,
    UPDATED = 2
  };
  const size_t MAX_VALUE_LEN = 512;
  const size_t MAX_KEY_LEN = 512;

  union FlagVersion
  {
    struct
    {
      FLAG_VERSION flag : FLAG_BITS;
      FLAG_VERSION version : VERSION_BITS;
    } fv;
    FLAG_VERSION pad;

    FlagVersion(FLAG_VERSION other)
    {
      pad = other;
    }

    FlagVersion()
    {
      fv.flag = 0;
      fv.version = 0;
    }

    FlagVersion &operator=(const FlagVersion &other)
    {
      fv.flag = other.fv.flag;
      fv.version = other.fv.version;
      return *this;
    }

    void set_flag(FLAG_t f)
    {
      fv.flag = static_cast<FLAG_VERSION>(f);
    }

    FLAG_t get_flag()
    {
      return static_cast<FLAG_t>(fv.flag);
    }

    void set_version(FLAG_VERSION version)
    {
      fv.version = version;
    }

    FLAG_VERSION get_version()
    {
      return fv.version;
    }
  };

#pragma pack(1)
  template <typename KEY, typename VALUE>
  class Pair_t
  {
  public:
    FlagVersion fv;
    KEY _key;
    VALUE _value;

    Pair_t()
    {
      _key = 0;
      _value = 0;
    };

    Pair_t(char *p)
    {
      auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
      *this = *pt;
    }

    void load(char *p)
    {
      auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
      *this = *pt;
    }

    KEY *key() { return &_key; }
    KEY str_key() { return _key; }
    VALUE value() { return _value; }
    size_t klen() { return sizeof(KEY); }
    Pair_t(KEY k, VALUE v)
    {
      _key = k;
      _value = v;
    }
    void set_key(KEY k) { _key = k; }

    void store_persist(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(KEY) + sizeof(VALUE);
      memcpy(addr + sizeof(FLAG_VERSION), p + sizeof(FLAG_VERSION), len);
      _mm_sfence();
      memcpy(addr, p, sizeof(FLAG_VERSION));
      clwb_sfence(addr, size());
    }

    void set_empty()
    {
      _key = 0;
      _value = 0;
    }

    void set_version(FLAG_VERSION version) { fv.set_version(version); }
    FLAG_VERSION get_version() { return fv.get_version(); }

    void set_flag(FLAG_t f) { fv.set_flag(f); }
    FLAG_t get_flag() { return fv.get_flag(); }
    void set_flag_persist(FLAG_t f)
    {
      fv.set_flag(f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }

    size_t size()
    {
      auto total_length = sizeof(FLAG_VERSION) + sizeof(KEY) + sizeof(VALUE);
      return total_length;
    }
  };

  template <typename KEY>
  class Pair_t<KEY, std::string>
  {
  public:
    FlagVersion fv;
    KEY _key;
    uint32_t _vlen;
    std::string svalue;

    Pair_t()
    {
      _vlen = 0;
      svalue.reserve(MAX_VALUE_LEN);
    };

    Pair_t(char *p)
    {
      auto pt = reinterpret_cast<Pair_t *>(p);
      _key = pt->_key;
      fv = pt->fv;
      _vlen = pt->_vlen;
      svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _vlen);
    }

    void load(char *p)
    {
      auto pt = reinterpret_cast<Pair_t *>(p);
      fv = pt->fv;
      _key = pt->_key;
      _vlen = pt->_vlen;
      svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _vlen);
    }

    size_t klen() { return sizeof(KEY); }
    KEY *key() { return &_key; }
    KEY str_key() { return _key; }
    std::string str_value() { return svalue; }

    Pair_t(KEY k, char *v_ptr, size_t vlen) : _vlen(vlen)
    {
      fv.set_version(0);
      _key = k;
      svalue.assign(v_ptr, _vlen);
    }

    void set_key(KEY k) { _key = k; }

    void store_persist(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(KEY) + sizeof(uint32_t);
      memcpy(addr + sizeof(FLAG_VERSION), p + sizeof(FLAG_VERSION), len);
      memcpy(addr + sizeof(FLAG_VERSION) + len, &svalue[0], _vlen);
      _mm_sfence();
      memcpy(addr, p, sizeof(FLAG_VERSION));
      clwb_sfence(addr, size());
    }

    void set_empty() { _vlen = 0; }
    void set_version(FLAG_VERSION version) { fv.set_version(version); }
    FLAG_VERSION get_version() { return fv.get_version(); }
    void set_flag(FLAG_t f) { fv.set_flag(f); }
    FLAG_t get_flag() { return fv.get_flag(); }
    void set_flag_persist(FLAG_t f)
    {
      fv.set_flag(f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }
    size_t size()
    {
      auto total_length =
          sizeof(KEY) + sizeof(uint32_t) + sizeof(FLAG_VERSION) + _vlen;
      return total_length;
    }
  };

  template <>
  class Pair_t<std::string, std::string>
  {
  public:
    FlagVersion fv;
    uint32_t _klen;
    uint32_t _vlen;
    std::string skey;
    std::string svalue;
    Pair_t()
    {
      _klen = 0;
      _vlen = 0;
      skey.reserve(MAX_KEY_LEN);
      svalue.reserve(MAX_VALUE_LEN);
    };
    Pair_t(char *p)
    {
      auto pt = reinterpret_cast<Pair_t *>(p);
      fv = pt->fv;
      _klen = pt->_klen;
      _vlen = pt->_vlen;
      skey.assign(p + 6, _klen);
      svalue.assign(p + 6 + _klen, _vlen);
    }
    void load(char *p)
    {
      auto pt = reinterpret_cast<Pair_t *>(p);
      fv = pt->fv;
      _klen = pt->_klen;
      _vlen = pt->_vlen;
      skey.assign(p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION), _klen);
      svalue.assign(p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(FLAG_VERSION) + _klen, _vlen);
    }
    size_t klen() { return _klen; }
    char *key() { return &skey[0]; }
    std::string str_key() { return skey; }
    std::string value() { return svalue; }

    Pair_t(char *k_ptr, size_t klen, char *v_ptr, size_t vlen)
        : _klen(klen), _vlen(vlen)
    {
      fv.set_version(0);
      skey.assign(k_ptr, _klen);
      svalue.assign(v_ptr, _vlen);
    }
    void set_key(char *k_ptr, size_t kl)
    {
      _klen = kl;
      skey.assign(k_ptr, _klen);
    }

    void store_persist(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(uint32_t) + sizeof(uint32_t);
      memcpy(addr + sizeof(FLAG_VERSION), p, len);
      memcpy(addr + sizeof(FLAG_VERSION) + len, &skey[0], _klen);
      memcpy(addr + sizeof(FLAG_VERSION) + len + _klen, &svalue[0], _vlen);
      _mm_sfence();
      memcpy(addr, p, sizeof(FLAG_VERSION));
      clwb_sfence(addr, size());
    }

    void set_empty()
    {
      _klen = 0;
      _vlen = 0;
    }
    void set_version(FLAG_VERSION version) { fv.set_version(version); }
    FLAG_VERSION get_version() { return fv.get_version(); }
    void set_flag(FLAG_t f) { fv.set_version(f); }
    FLAG_t get_flag() { return fv.get_flag(); }
    void set_flag_persist(FLAG_t f)
    {
      fv.set_flag(f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }

    size_t size()
    {
      auto total_length = sizeof(uint32_t) + sizeof(uint32_t) +
                          sizeof(FLAG_VERSION) + _klen + _vlen;
      return total_length;
    }
  };
#pragma pack()
}