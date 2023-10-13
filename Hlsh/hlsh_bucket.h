#include "hlsh_pm_manage.h"

namespace HLSH_hashing{
  constexpr int rSuccess = 1;
  constexpr int rFailure = 0;
  constexpr int rFailureGetLock = -2;
  constexpr int rNoEmptySlot = -3;
  constexpr int rSegmentChanged = -4;

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
  // constexpr uint16_t kBucketSpareSlotNum = 10;
  // constexpr uint16_t kBucketTotalSlotNum = kBucketNormalSlotNum + kBucketSpareSlotNum;
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
    uint64_t key;
    PmOffset value;

    _Pair() {
      key = 0;
      value = PO_NULL;
    }
    _Pair(uint64_t _key,PmOffset _value){
      key = _key;
      value = _value;
    }
  } PACKED;

  struct Bitmap
  {
    uint64_t b;

    Bitmap() { b = 0; }

    inline void Init(){ b = 0; }

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
    Lock8 lock[kSpareBucketNum];    // 8B
    uint8_t pad[8-kSpareBucketNum];   
    Bitmap bitmap[kSpareBucketNum];  // 56B
    uint64_t pad1[7 - kSpareBucketNum]; // 24B
    _Pair _[kSparePairNum];

    static size_t GetSize() { return kSparePairNum; }

    inline void ClearBit(size_t pos)
    {
      bitmap[pos >> 6].UnsetBit(pos & 63);
    }

    SpareBucket()
    {
      for (size_t i = 0; i < kSpareBucketNum; i++)
      {
        lock[i].Init();
        bitmap[i].Init();
      }
    }

    inline void Init() {
      for (size_t i = 0; i < kSpareBucketNum; i++) {
        lock[i].Init();
        bitmap[i].Init();
      }
    }

    inline int Insert(uint64_t key, PmOffset value, uint8_t finger)
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
          _[real_slot].key = key;
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

    bool Delete(Pair_t<KEY, VALUE> *p, size_t key_hash, uint64_t position,
                PmManage<KEY, VALUE> *pm)
    {
      // s1: delete kv for varied-length kv
      if (_[position].key == key_hash) {
        auto o = _[position].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE>*>(o.GetValue());
        // s1.1 delete kv if key is equal
        if (v->str_key() == p->str_key()) {
          pm->Delete(_[position].value);
          auto m = position >> 6;
          lock[m].GetLock();
          bitmap[m].UnsetBit(position & 63);
          lock[m].ReleaseLock();
          return rSuccess;
        }
      }
      return rFailure;
    }

    inline bool FindDuplicate(Pair_t<KEY, VALUE>* p, uint64_t key_hash,
                              uint64_t pos) {
      if (_[pos].key == key_hash) {
        auto o = _[pos].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE>*>(o.GetValue());
        if (v->str_key() == p->str_key()) {
          return rSuccess;
        }
      }
      return rFailure;
    }

    inline int Get(Pair_t<KEY, VALUE>* p, uint64_t key_hash, uint64_t pos) {
      if (_[pos].key == key_hash)
      {
        auto o = _[pos].value;
        // auto addr = reinterpret_cast<char *>(o.chunk_start_addr + o.offset);
        // pv[GetNum].kp.push_back(addr);
        // _mm_prefetch(addr,_MM_HINT_NTA);
        auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
        if (v->str_key() == p->str_key())
        {
          p->load((char *)v);
          return rSuccess;
        }
      }
      return rFailure;
    }

    bool Update(Pair_t<KEY, VALUE>* p, uint64_t key_hash, size_t thread_id,
                uint64_t position, PmManage<KEY,VALUE>* pm) {
      if (_[position].key == key_hash) {
        // s2.1.1: obtain pointer to kv
        auto o = _[position].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE>*>(o.GetValue());
        if (p->str_key() == v->str_key()) {
          _[position].value = pm->Update(p, thread_id, _[position].value);
          return rSuccess;
        }
      }
      return rFailure;
    }

    bool UpdateForReclaim(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                          PmOffset old_value, PmOffset new_value,
                          uint64_t position, PmManage<KEY, VALUE> *pm)
    {
      if (_[position].key == key_hash)
      {
        // s2.1.1: obtain pointer to kv
        if (_[position].value == old_value)
        {
          _[position].value = new_value;
        };
      }
      return rFailure;
    }
  } PACKED;

  template <class KEY, class VALUE>
  struct BucketFirstLine
  {
    VersionLock16 lock; //2B
    uint16_t bitmap;//2B
    uint8_t fingers[12];//12
    PmOffset value[kBucketNormalSlotNum];//48B

    BucketFirstLine()
    {
      lock.Init();
      bitmap = 0;
    }

    inline void Init() 
    {
      lock.Init();
      bitmap = 0;
    }

    inline int FindEmptySlot()
    {
      auto pos = __builtin_ctz(~bitmap);
      if (pos < 12)
        return pos;
      return rNoEmptySlot;
    }
  } PACKED;

  struct FSPos{
    uint16_t finger:8;
    uint16_t spos : 8;
  } PACKED;

  template <class KEY, class VALUE>
  struct BucketSecondLine
  {
    uint8_t bitmap;//2B
    uint8_t pad;
    FSPos spos[7];  // 14B
    uint64_t key[kBucketNormalSlotNum];//48B

    BucketSecondLine()
    {
      bitmap = 0;
    }

    inline void Init()
    {
      bitmap = 0;
    }

    inline int FindEmptySlot()
    {
      auto pos = __builtin_ctz(~bitmap);
      if (pos < 7)
        return pos;
      return rNoEmptySlot;
    }
  } PACKED;

  template <class KEY, class VALUE>
  struct Bucket
  {
    BucketFirstLine<KEY, VALUE> bf;
    BucketSecondLine<KEY, VALUE> bs;

    Bucket() {}

    inline void Init()
    {
      bf.Init();
      bs.Init();
    }

    inline int GetCount()
    {
      if (!CHECK_BIT(bf.bitmap, 12))
      {
        return __builtin_popcount(bf.bitmap);
      }
      else
      {
        auto b = bf.bitmap & (~(1 << 12));
        return __builtin_popcount(b) + __builtin_popcount(bs.bitmap);
      }
    }

    static size_t GetSlotNum() { return kBucketNormalSlotNum; }

    bool FindDuplicate(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                       uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(bf.fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bf.bitmap;
      // s: find duplicate kv
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            // s: search in normal bucket
            auto o = bf.value[pos];
            auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
            if (p->str_key() == v->str_key())
            {
              return rSuccess;
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->FindDuplicate(p, key_hash,
                                       bf.value[pos - kBucketNormalSlotNum].spos);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (mask);
      }
      // s: check spare slot
      if (CHECK_BIT(bf.bitmap, 12))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(bs.bitmap, i) && bs.spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->FindDuplicate(p, key_hash, bs.spos[i].spos);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
        }
      }
      return rFailure;
    }

    bool UpdateForReclaim(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                          PmOffset old_value, PmOffset new_value,
                          uint8_t finger, PmManage<KEY, VALUE> *pm,
                          SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(bf.fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bf.bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            // s: search in normal bucket
            if (bf.value[pos] == old_value)
            {
              bf.value[pos] == new_value;
              return rSuccess;
            };
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->UpdateForReclaim(p, key_hash,
                                          old_value, new_value,
                                          bf.value[pos - kBucketNormalSlotNum].spos,
                                          pm);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (mask);
      }
      // s: check spare slot
      if (CHECK_BIT(bf.bitmap, 12))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(bs.bitmap, i) && bs.spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->UpdateForReclaim(p, key_hash,
                                          old_value, new_value,
                                          bs.spos[i].spos, pm);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
        }
      }
      pm->Delete(old_value);
      return rFailure;
    }

    bool Update(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
                PmManage<KEY, VALUE> *pm, size_t thread_id, SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(bf.fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bf.bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            // s: search in normal bucket
            auto o = bf.value[pos];
            auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
            if (p->str_key() == v->str_key())
            {
              bf.value[pos] = pm->Update(p, thread_id, bf.value[pos]);
              return rSuccess;
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->Update(p, key_hash, thread_id, bf.value[pos - kBucketNormalSlotNum].spos, pm);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (mask);
      }
      // s: check spare slot
      if (CHECK_BIT(bf.bitmap, 12))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(bs.bitmap, i) && bs.spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->Update(p, key_hash, thread_id, bs.spos[i].spos, pm);
            if (rSuccess == r)
            {
              return rSuccess;
            }
          }
        }
      }
      return rFailure;
    }

    inline int Get(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
            SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(bf.fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bf.bitmap;
      /* s: search normal and spare bucket */
      if (mask)
      {
        auto num = __builtin_popcount(mask);
        bool check_key_hash = false;
        if (num > 1)
          check_key_hash = true;
        do
        {
          // return rSuccess;
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            if (LIKELY(!check_key_hash))
            {
              // s: search in normal bucket
              auto o = bf.value[pos];
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key())
              {
                count++;
                p->load((char*)(v));
                return rSuccess;
              }
            }
            else
            {
              if (bs.key[pos] == key_hash)
              {
                // s: search in normal bucket
                auto o = bf.value[pos];
                auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
                if (p->str_key() == v->str_key())
                {
                  count2++;
                  p->load((char *)(v));
                  return rSuccess;
                }
              }
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->Get(p, key_hash, bf.value[pos - kBucketNormalSlotNum].spos);
            if (rSuccess == r)
            {
              count1++;
              return rSuccess;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (UNLIKELY(mask));
      }
      // s: check spare slot
      if (UNLIKELY(CHECK_BIT(bf.bitmap, 12)))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(bs.bitmap, i) && bs.spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->Get(p, key_hash, bs.spos[i].spos);
            if (rSuccess == r){
              count2++;
              return rSuccess;
            }
          }
        }
      }
      return rFailure;
    }

  inline int Insert(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
         PmManage<KEY, VALUE> *pm, size_t thread_id, SpareBucket<KEY, VALUE> *sb)
    {
      // s1: check whether exist duplicate key-value;
      if (FindDuplicate(p, key_hash, finger, sb))
        return rSuccess;
      // s2: insert key-value to pm
      if (tl_value == PO_NULL)
        tl_value = pm->Insert(p, thread_id);
      // s2: get empy slot if exist
      auto pos = FindEmptySlot();
      if (rNoEmptySlot == pos)
      {
        return rNoEmptySlot;
      }
      // s3: insert kv to bucket or stash
      if (pos < kBucketNormalSlotNum)
      {
        // s: insert into slot in current bucket
        bf.value[pos] = tl_value;
        bs.key[pos] = key_hash;
        bf.fingers[pos] = finger;
        SET_BIT16(bf.bitmap, pos);
      }
      else
      {
        // s: insert key and offset into stash
        auto k = sb->Insert(key_hash, tl_value, finger);
        if (rNoEmptySlot != k)
        {
          if (pos < 12)
          {
            bf.fingers[pos] = finger;
            bf.value[pos - kBucketNormalSlotNum].spos = k;
            SET_BIT16(bf.bitmap, pos);
          }
          else
          {
            auto q = pos - 12;
            bs.spos[q].finger = finger;
            bs.spos[q].spos = k;
            SET_BIT8(bs.bitmap, q);
            SET_BIT16(bf.bitmap, 12);
          }
        }
        else
        {
          return rNoEmptySlot;
        }
      }
      return rSuccess;
    }

    inline int FindEmptySlot()
    {
      auto pos = bf.FindEmptySlot();
      if (pos != rNoEmptySlot)
        return pos;
      pos = bs.FindEmptySlot();
      if (pos != rNoEmptySlot)
        return 12 + pos;
      return rNoEmptySlot;
    }

    inline int Insert(uint64_t key, PmOffset value, uint8_t finger,
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
        bf.value[pos] = value;
        bs.key[pos] = key;
        // s: update finger and bitmap
        bf.fingers[pos] = finger;
        bf.bitmap |= (1 << pos);
      }
      else
      {
        /* s: insert into spare bucket */
        auto k = sb->Insert(key, value, finger);
        if (rNoEmptySlot != k)
        {
          // s: update finger, spos, bitmap
          if (pos < 12)
          {
            bf.fingers[pos] = finger;
            bf.value[pos - kBucketNormalSlotNum].spos = k;
            SET_BIT16(bf.bitmap, pos);
          }
          else
          {
            auto q = pos - 12;
            bs.spos[q].finger = finger;
            bs.spos[q].spos = k;
            SET_BIT8(bs.bitmap, q);
            SET_BIT16(bf.bitmap, 12);
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
    int Delete(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
               PmManage<KEY, VALUE> *pm, SpareBucket<KEY, VALUE> *sb)
    {
      // s: compare finger with SIMD
      auto mask = CMP128(bf.fingers, finger);
      MASK(mask, 12);
      // s: filter with bitmap
      mask &= bf.bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            // s: search in normal bucket
            auto o = bf.value[pos];
            auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
            if (p->str_key() == v->str_key())
            {
              pm->Delete(bf.value[pos]);
              UNSET_BIT16(bf.bitmap, pos);
              return rSuccess;
            }
          }
          else if (pos < 12)
          {
            // s: search in spare bucket
            auto r = sb->Delete(p, key_hash, bf.value[pos - kBucketNormalSlotNum].spos, pm);
            if (rSuccess == r)
            {
              UNSET_BIT16(bf.bitmap, pos);
              return rSuccess;
            }
          }
          UNSET_BIT16(mask, pos);
        } while (mask);
      }
      // s: check spare slot
      if (CHECK_BIT(bf.bitmap, 12))
      {
        for (size_t i = 0; i < 7; i++)
        {
          if (CHECK_BIT(bs.bitmap, i) && bs.spos[i].finger == finger)
          {
            // s: search in spare bucket
            auto r = sb->Delete(p, key_hash, bs.spos[i].spos, pm);
            if (rSuccess == r)
            {
              UNSET_BIT8(bs.bitmap, i);
              if (!bs.bitmap)
              {
                UNSET_BIT16(bf.bitmap, 12);
              }
              return rSuccess;
            }
          }
        }
      }
      return rFailure;
    }
  } PACKED;
}