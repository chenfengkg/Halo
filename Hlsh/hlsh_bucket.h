#include "hlsh_pm_manage.h"

namespace HLSH_hashing
{
  constexpr int rSuccess = -1;
  constexpr int rFailure = -2;
  constexpr int rFailureGetLock = -3;
  constexpr int rNoEmptySlot = -4;
  constexpr int rSegmentChanged = -5;

  constexpr size_t kFingerBits = 8;
  constexpr size_t kMask = (1 << kFingerBits) - 1;
#define KEY_FINGER(hash) ((hash) & kMask);
#define BUCKET_INDEX(hash) ((hash >> kFingerBits) & bucketMask)

#define SET_BIT16(m, b) (m |= (uint16_t)(1 << (b)))
#define UNSET_BIT16(m, b) (m &= (~((uint16_t)(1 << (b)))))
#define SET_BIT8(m, b) (m |= (uint8_t)(1 << (b)))
#define UNSET_BIT8(m, b) (m &= (~((uint8_t)(1 << (b)))))
#define UNSET_BITS(m, b) (m &= (~(b)))
#define MASK(m, b) (m &= ((1 << (b)) - 1))

  constexpr size_t kNumBucket = 128; /* the number of normal buckets in one segment*/

  constexpr size_t kSpareBucketNum = 3;
  constexpr size_t kSparePairNum = 64 * kSpareBucketNum; /* the number of stash buckets in one segment*/

  constexpr uint16_t kBucketNormalSlotNum = 6;
  constexpr uint16_t SlotMask = 15;
  constexpr uint16_t kBucketFull = (1 << 12) - 1;

  constexpr size_t bucketMask = kNumBucket - 1;

  std::atomic<uint64_t> count = {0};
  std::atomic<uint64_t> count1 = {0};
  std::atomic<uint64_t> count2 = {0};
  std::atomic<uint64_t> count3 = {0};
  std::atomic<uint64_t> count4 = {0};
  std::atomic<uint64_t> count5 = {0};

  struct PrefetchValue
  {
    char *ok;               // origin kv
    std::vector<char *> kp; // possible key

    PrefetchValue()
    {
      ok = nullptr;
    }

    void Clear()
    {
      ok = nullptr;
      kp.clear();
    }
  };

  // constexpr size_t PrefetchNum = 12;
  constexpr size_t PrefetchNum = 12;
  thread_local PrefetchValue pv[PrefetchNum];
  thread_local size_t GetNum = 0;

  struct _Pair
  {
    uint64_t hkey;
    PmOffset value;

    _Pair()
    {
      hkey = 0;
      value = PO_NULL;
    }
    _Pair(uint64_t key_hash, PmOffset _value)
    {
      hkey = key_hash;
      value = _value;
    }
  } PACKED;

  struct Bitmap
  {
    uint64_t b;

    Bitmap() { b = 0; }

    inline void Init() { b = 0; }

    inline int FindEmptySlot()
    {
      if (b != (uint64_t)-1)
        return __builtin_ctzl(~b);
      return rNoEmptySlot;
    }

    inline void SetBit(uint64_t index)
    {
      b = b | (1UL << index);
    }

    inline void UnsetBit(uint64_t index)
    {
      b = b & (~(1UL << index));
    }

  } PACKED;

  template <class KEY, class VALUE>
  struct SpareBucket
  {
    Lock8 lock[kSpareBucketNum]; // 8B
    uint8_t pad[8 - kSpareBucketNum];
    Bitmap bitmap[kSpareBucketNum];     // 56B
    uint64_t pad1[7 - kSpareBucketNum]; // 24B
    _Pair _[kSparePairNum];

    static size_t GetSize() { return kSparePairNum; }

    inline void ClearBit(size_t pos)
    {
      bitmap[pos >> 6].UnsetBit(pos & 63);
    }

    SpareBucket()
    {
      memset(bitmap, 0, sizeof(Bitmap) * kSpareBucketNum);
    }

    inline void Init()
    {
      for (size_t i = 0; i < kSpareBucketNum; i++)
      {
        lock[i].Init();
        bitmap[i].Init();
      }
    }

    inline int Insert(uint64_t key_hash, PmOffset value, uint8_t finger)
    {
      // s: loop traverse bucket
      for (size_t i = 0; i < kSpareBucketNum; i++)
      {
        auto m = ((finger % kSpareBucketNum) + i) % kSpareBucketNum;
        lock[m].GetLock();
        // s1: find empty slot
        auto slot = bitmap[m].FindEmptySlot();
        // s2: insert key-value if empty slot exist
        if (rNoEmptySlot != slot)
        {
          // s2.1: update slot position
          auto real_slot = slot + m * 64;
          // s2.2: insert key-value
          _[real_slot].value = value;
          _[real_slot].hkey = key_hash;
          bitmap[m].SetBit(slot);
          lock[m].ReleaseLock();
          return real_slot;
        }
        else
        {
          // s2.2: no emtpy slot
          lock[m].ReleaseLock();
        }
      }
      return rNoEmptySlot;
    }

    bool Delete(Pair_t<KEY, VALUE> *p, size_t key_hash,
                uint64_t position, PmManage<KEY, VALUE> *pm)
    {
      // s1: delete kv for varied-length kv
      if (_[position].hkey == key_hash)
      {
        auto o = _[position].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        // s1.1 delete kv if key is equal
        if (v->str_key() == p->str_key())
        {
          pm->Delete(_[position].value);
          auto m = position >> 6;
          lock[m].GetLock();
          bitmap[m].UnsetBit(position & 63);
          lock[m].ReleaseLock();
          return true;
        }
      }
      return false;
    }

    inline bool FindDuplicate(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                              uint64_t pos)
    {
      if (_[pos].hkey == key_hash)
      {
        auto o = _[pos].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (v->str_key() == p->str_key())
        {
          return true;
        }
      }
      return false;
    }

    inline bool Get(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint64_t pos)
    {
      if (_[pos].hkey == key_hash)
      {
        auto o = _[pos].value;
        // auto addr = reinterpret_cast<char *>(o.chunk_start_addr + o.offset);
        // pv[GetNum].kp.push_back(addr);
        // _mm_prefetch(addr,_MM_HINT_NTA);
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (v->str_key() == p->str_key())
        {
          p->load((char *)v);
          return true;
        }
      }
      return false;
    }

    inline bool Update(Pair_t<KEY, VALUE> *p, uint64_t key_hash, size_t thread_id,
                       uint64_t position, PmManage<KEY, VALUE> *pm)
    {
      if (_[position].hkey == key_hash)
      {
        // s2.1.1: obtain pointer to kv
        auto o = _[position].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (p->str_key() == v->str_key())
        {
          _[position].value = pm->Update(p, thread_id, _[position].value);
          return true;
        }
      }
      return false;
    }

    inline bool UpdateForReclaim(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                                 PmOffset old_value, PmOffset new_value,
                                 uint64_t position, PmManage<KEY, VALUE> *pm)
    {
      if (_[position].hkey == key_hash)
      {
        // s2.1.1: obtain pointer to kv
        if (_[position].value == old_value)
        {
          _[position].value = new_value;
          return true;
        };
      }
      return false;
    }
  } PACKED;


  struct FSPos
  {
    uint16_t finger : 8;
    uint16_t spos : 8;
  } PACKED;

  template <class KEY, class VALUE>
  struct Bucket
  {
    VersionLock16 lock;                   // 2B
    uint16_t bitmap;                      // 2B
    uint8_t fingers[12];                  // 12
    _Pair slot[kBucketNormalSlotNum];     // 6*16B
    uint8_t sbitmap;  // 1B
    uint8_t pad;  //1B
    FSPos spos[7];   // 14B

    Bucket() {}

    inline void Init()
    {
      lock.Init();
      bitmap = 0;
      sbitmap = 0;
    }

    inline int GetCount()
    {
      if (!CHECK_BIT(bitmap, 12))
      {
        return __builtin_popcount(bitmap);
      }
      else
      {
        auto b = bitmap & (~(1 << 12));
        return __builtin_popcount(b) + __builtin_popcount(sbitmap);
      }
    }

    static size_t GetSize() { return kBucketNormalSlotNum; }

    int FindDuplicate(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                      uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bitmap;
      // s: find duplicate kv
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            if (slot[pos].hkey == key_hash)
            {
              // s: search in normal bucket
              auto o = slot[pos].value;
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key())
              {
                return pos;
              }
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->FindDuplicate(p, key_hash, slot[pos - kBucketNormalSlotNum].value.spos);
            if (r)
              return pos;
          }
          UNSET_BIT16(mask, pos);
        } while (mask);
      }
      // s: check spare slot
      if (CHECK_BIT(bitmap, 12))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(sbitmap, i) && spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->FindDuplicate(p, key_hash, spos[i].spos);
            if (r)
              return (12 + i);
          }
        }
      }
      return rFailure;
    }

    bool UpdateForReclaim(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                          PmOffset old_value, PmOffset new_value,
                          uint8_t finger, PmManage<KEY, VALUE> *pm,
                          SpareBucket<KEY, VALUE> *sb, int pos)
    {
      if (pos < kBucketNormalSlotNum)
      {
        // s: search in normal bucket
        if (slot[pos].value == old_value)
        {
          slot[pos].value == new_value;
        };
      }
      else if (pos < 12)
      {
        // s: search in spare bucket
        sb->UpdateForReclaim(p, key_hash, old_value, new_value,
                             slot[pos - kBucketNormalSlotNum].value.spos,
                             pm);
      }
      else
      {
        // s: search in spare bucket
        sb->UpdateForReclaim(p, key_hash, old_value, new_value,
                             spos[pos - 12].spos, pm);
      }
      pm->Delete(old_value);
      return true;
    }

    bool Update(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                uint8_t finger, PmManage<KEY, VALUE> *pm,
                size_t thread_id, SpareBucket<KEY, VALUE> *sb,
                int pos)
    {
      if (pos < kBucketNormalSlotNum)
      {
        // s: search in normal bucket
        auto o = slot[pos].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (p->str_key() == v->str_key())
        {
          slot[pos].value = pm->Update(p, thread_id, slot[pos].value);
        }
      }
      else if (pos < 12)
      {
        // s: search in spare bucket
        sb->Update(p, key_hash, thread_id,
                   slot[pos - kBucketNormalSlotNum].value.spos, pm);
      }
      else
      {
        // s: search in spare bucket
        sb->Update(p, key_hash, thread_id, spos[pos - 12].spos, pm);
      }
      return true;
    }

    inline int Get(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                   uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bitmap;
      /* s: search normal and spare bucket */
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            if (slot[pos].hkey == key_hash)
            {
              // s: search in normal bucket
              auto o = slot[pos].value;
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key())
              {
                p->load((char *)(v));
                return true;
              }
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->Get(p, key_hash, slot[pos - kBucketNormalSlotNum].value.spos);
            if (r)
            {
              return true;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (UNLIKELY(mask));
      }
      // s: check spare slot
      if (UNLIKELY(CHECK_BIT(bitmap, 12)))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(sbitmap, i) &&
              spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->Get(p, key_hash, spos[i].spos);
            if (r)
            {
              return rSuccess;
            }
          }
        }
      }
      return false;
    }

    inline int FindEmptySlot()
    {
      auto pos = __builtin_ctz(~bitmap);
      if (pos < 12)
        return pos;
      pos = __builtin_ctz(~sbitmap);
      if (pos < 7)
        return 12 + pos;
      return rNoEmptySlot;
    }

    inline int Insert(uint64_t key_hash, PmOffset value, uint8_t finger,
                      SpareBucket<KEY, VALUE> *sb)
    {
      // s1: get empy slot if exist
      auto pos = FindEmptySlot();
      if (pos == rNoEmptySlot)
      {
        return rNoEmptySlot;
      }
      /* s2: insert kv to normal bucket or spare bucket */
      if (pos < kBucketNormalSlotNum)
      {
        /* s: insert into normal bucket */
        // s: insert key and offset in current bucket
        slot[pos].value = value;
        slot[pos].hkey = key_hash;
        // s: update finger and bitmap
        fingers[pos] = finger;
        SET_BIT16(bitmap, pos);
      }
      else
      {
        /* s: insert into spare bucket */
        auto k = sb->Insert(key_hash, value, finger);
        if (rNoEmptySlot != k)
        {
          // s: update finger, spos, bitmap
          if (pos < 12)
          {
            slot[pos - kBucketNormalSlotNum].value.spos = k;
            fingers[pos] = finger;
            SET_BIT16(bitmap, pos);
          }
          else
          {
            auto q = pos - 12;
            spos[q].finger = finger;
            spos[q].spos = k;
            SET_BIT8(sbitmap, q);
            SET_BIT16(bitmap, 12);
          }
        }
        else
        {
          return rNoEmptySlot;
        }
      }
      return rSuccess;
    }

    /*if delete success, then return 0, else return -1*/
    bool Delete(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                uint8_t finger, PmManage<KEY, VALUE> *pm,
                SpareBucket<KEY, VALUE> *sb, int pos)
    {
      if (pos < kBucketNormalSlotNum)
      {
        // s: search in normal bucket
        auto o = slot[pos].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (p->str_key() == v->str_key())
        {
          UNSET_BIT16(bitmap, pos);
          pm->Delete(slot[pos].value);
        }
      }
      else if (pos < 12)
      {
        // s: search in spare bucket
        auto r = sb->Delete(p, key_hash, slot[pos - kBucketNormalSlotNum].value.spos, pm);
        if (r)
        {
          UNSET_BIT16(bitmap, pos);
        }
      }
      else
      {
        // s: search in spare bucket
        auto r = sb->Delete(p, key_hash, spos[pos - 12].spos, pm);
        if (r)
        {
          UNSET_BIT8(sbitmap, (pos - 12));
          if (!sbitmap)
          {
            UNSET_BIT16(bitmap, 12);
          }
        }
      }
      return true;
    }
  } PACKED;
}