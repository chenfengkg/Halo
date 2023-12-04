#include "hlsh_utils.h"

namespace HLSH_hashing
{
  using FVERSION = uint8_t;
  #define PAIR_VALUE_LEN 512
  #define PAIR_KEY_LEN 8
  constexpr uint8_t kInvalidPair = 0xC0;
  constexpr uint8_t kFlagBits = 2;
  constexpr uint8_t kFlagMask = (1 << kFlagBits) - 1;
  constexpr uint8_t kVersionBits = sizeof(FVERSION) * 8 - kFlagBits;
  constexpr uint8_t kVMask = (1 << kVersionBits) - 1;
#define FVERSION_SET_VERSION(ov, nv) (ov = (((ov) & (~((1 << kVersionBits) - 1))) | nv))
#define FVERSION_GET_VERSION(ov) ((ov) & ((1 << kVersionBits) - 1))
#define FVERSION_SET_FLAG(of, nf) (of = (((of) & (~(kFlagMask << kVersionBits))) | ((nf) << kVersionBits)))
#define FVERSION_GET_FLAG(of) (((of) >> kVersionBits) & kFlagMask)

  enum FLAG_t
  {
    INVALID = 0,
    VALID = 1,
    UPDATED = 2,
    END = 3
  };
  const size_t MAX_VALUE_LEN = 512;
  const size_t MAX_KEY_LEN = 512;

#pragma pack(1)
  template <typename KEY, typename VALUE>
  class Pair_t
  {
  public:
    FVERSION fv;
    KEY _key;
    VALUE _value;

    Pair_t()
    {
      fv = 0;
      _key = 0;
      _value = 0;
    };

    Pair_t(char *p)
    {
      auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
      *this = *pt;
    }

    Pair_t(KEY k, VALUE v)
    {
      // s: set pair valid
      fv = 1 << kVersionBits;
      _key = k;
      _value = v;
    }

    inline bool IsValid()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::VALID);
    }

    inline bool IsEnd()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::END);
    }

    inline bool cmp_key(Pair_t<KEY, VALUE> *other)
    {
      return _key == other->_key;
    }

    inline bool cmp_key_and_load(Pair_t<KEY, VALUE> *other)
    {
      if (_key == other->_key)
      {
        other->_value = _value;
        return true;
      }
      return false;
    }

    inline KEY *key() { return &_key; }
    inline size_t klen() { return sizeof(KEY); }

    inline void set_key(KEY k) { _key = k; }

    inline void store_persist(char *addr)
    {
      // s: copy other data except fv
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(KEY) + sizeof(VALUE);
      memcpy(addr + sizeof(FVERSION), p + sizeof(FVERSION), len);
      // s: set next pair flag as invalid
      *(reinterpret_cast<uint8_t *>(addr + sizeof(FVERSION) + len)) = kInvalidPair;
      // s: barrier
      _mm_sfence();
      memcpy(addr, p, sizeof(FVERSION));
      clwb_sfence(addr, size() + sizeof(uint8_t));
    }

    inline void store(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      memcpy(addr, p, size());
    }

    inline void update(Pair_t<KEY, VALUE> *p)
    {
      _value = p->_value;
      clwb_sfence(&_value, sizeof(VALUE));
    }

    void set_empty()
    {
      _key = 0;
      _value = 0;
    }

    void set_version(FVERSION version)
    {
      FVERSION_SET_VERSION(fv, version);
    }

    FVERSION get_version()
    {
      return FVERSION_GET_VERSION(fv);
    }

    void set_flag(FVERSION f)
    {
      FVERSION_SET_FLAG(fv, f);
    }

    FVERSION get_flag()
    {
      return FVERSION_GET_FLAG(fv);
    }

    void set_flag_persist(FVERSION f)
    {
      FVERSION_SET_FLAG(fv, f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }

    inline size_t size()
    {
      return sizeof(FVERSION) + sizeof(KEY) + sizeof(VALUE);
    }
  };

  template <typename KEY>
  class Pair_t<KEY, std::string>
  {
  public:
    FVERSION fv;
    KEY _key;
    uint32_t _vlen;
    char svalue[PAIR_VALUE_LEN];

    Pair_t<KEY,std::string>(){
      fv = 0;
      _vlen = 0;
    }

    inline bool IsValid()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::VALID);
    }

    inline bool IsEnd()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::END);
    }

    inline bool cmp_key(Pair_t<KEY, std::string> *other)
    {
      return other->_key == _key;
    }

    inline bool cmp_key_and_load(Pair_t<KEY, std::string> *other)
    {
      if (other->_key == _key)
      {
        other->_vlen = _vlen;
        memcpy(other->svalue, svalue, _vlen);
        return true;
      }
      return false;
    }

    size_t klen() { return sizeof(KEY); }
    KEY *key() { return &_key; }

    Pair_t(KEY k, char *v_ptr, size_t vlen) : _vlen(vlen), _key(k)
    {
      FVERSION_SET_VERSION(fv, 0);
      memcpy(svalue, v_ptr, vlen);
    }

    void set_key(KEY k) { _key = k; }

    void store_persist(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(KEY) + sizeof(uint32_t);
      memcpy(addr + sizeof(FVERSION), p + sizeof(FVERSION), len);
      memcpy(addr + sizeof(FVERSION) + len, &svalue[0], _vlen);
      // s: set next pair flag as invalid
      *(reinterpret_cast<uint8_t *>(addr + sizeof(FVERSION) + len + _vlen)) = kInvalidPair;
      // s: barrier
      _mm_sfence();
      memcpy(addr, p, sizeof(FVERSION));
      clwb_sfence(addr, size() + sizeof(uint8_t));
    }

    void store(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      memcpy(addr, p, size());
    }

    inline bool update(Pair_t<KEY, std::string> *p)
    {
      return false;
    }

    inline void set_empty() { _vlen = 0; }
    inline void set_version(FVERSION version)
    {
      FVERSION_SET_VERSION(fv, version);
    }
    inline FVERSION get_version() { return FVERSION_GET_VERSION(fv); }
    inline void set_flag(FVERSION f)
    {
      FVERSION_SET_FLAG(fv, f);
    }
    inline FLAG_t get_flag()
    {
      return FVERSION_GET_FLAG(fv);
    }

    inline void set_flag_persist(FVERSION f)
    {
      FVERSION_SET_FLAG(fv, f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }
    inline size_t size()
    {
      return sizeof(KEY) + sizeof(uint32_t) + sizeof(FVERSION) + _vlen;
    }
  };

  template <>
  class Pair_t<std::string, std::string>
  {
  public:
    FVERSION fv;
    uint32_t _klen;
    uint32_t _vlen;
    char svalue[PAIR_KEY_LEN + PAIR_VALUE_LEN];

    Pair_t()
    {
      fv = 0;
      _klen = 0;
      _vlen = 0;
    };

    Pair_t(char *p)
    {
      auto pt = reinterpret_cast<Pair_t *>(p);
      fv = pt->fv;
      _klen = pt->_klen;
      _vlen = pt->_vlen;
      auto len = sizeof(FVERSION) + sizeof(uint32_t) + sizeof(uint32_t);
      memcpy(svalue, p + len, _klen + _vlen);
    }

    inline bool IsValid()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::VALID);
    }

    inline bool IsEnd()
    {
      return (FVERSION_GET_FLAG(fv) == FLAG_t::END);
    }

    inline bool cmp_key(Pair_t<std::string, std::string> *other)
    {
      return (_klen == other->_klen) && (!std::strncmp(svalue, other->svalue, _klen));
    }

    inline bool cmp_key_and_load(Pair_t<std::string, std::string> *other)
    {
      if (_klen == other->_klen &&
          (!std::strncmp(svalue, other->svalue, _klen)))
      {
        other->_vlen = _vlen;
        memcpy(other->svalue + _klen, svalue + _klen, _vlen);
        return true;
      }
      return false;
    }

    size_t klen() { return _klen; }
    char *key() { return svalue; }

    Pair_t(char *k_ptr, size_t klen, char *v_ptr, size_t vlen)
        : _klen(klen), _vlen(vlen)
    {
      FVERSION_SET_VERSION(fv,0);
      memcpy(svalue, k_ptr, _klen);
      memcpy(svalue + _klen, v_ptr, _vlen);
    }

    void set_key(char *k_ptr, size_t kl)
    {
      _klen = kl;
      memcpy(svalue, k_ptr, _klen);
    }

    void store_persist(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      auto len = sizeof(uint32_t) + sizeof(uint32_t);
      memcpy(addr + sizeof(FVERSION), p, len);
      memcpy(addr + sizeof(FVERSION) + len, svalue, _klen + _vlen);
      // s: set next pair flag as invalid
      *(reinterpret_cast<uint8_t *>(addr + sizeof(FVERSION) + len + _klen + _vlen)) = kInvalidPair;
      // s: barrier
      _mm_sfence();
      memcpy(addr, p, sizeof(FVERSION));
      clwb_sfence(addr, size() + sizeof(uint8_t));
    }

    void store(char *addr)
    {
      auto p = reinterpret_cast<char *>(this);
      memcpy(addr, p, size());
    }

    inline bool update(Pair_t<std::string, std::string> *p)
    {
      return false;
    }

    void set_empty()
    {
      _klen = 0;
      _vlen = 0;
    }

    void set_version(FVERSION version) { FVERSION_SET_VERSION(fv, version); }

    FVERSION get_version() { return FVERSION_GET_VERSION(fv); }

    void set_flag(FVERSION f) { FVERSION_SET_FLAG(fv, f); }

    FVERSION get_flag() { return FVERSION_GET_FLAG(fv); }

    void set_flag_persist(FVERSION f)
    {
      FVERSION_SET_FLAG(fv,f);
      clwb_sfence(reinterpret_cast<char *>(this), 8);
    }

    size_t size()
    {
      return sizeof(uint32_t) + sizeof(uint32_t) +
             sizeof(FVERSION) + _klen + _vlen;
    }
  };
#pragma pack()
}