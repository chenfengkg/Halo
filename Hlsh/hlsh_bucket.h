#include "hlsh_pm_manage.h"

namespace HLSH_hashing{
  constexpr int rSuccess = 1;
  constexpr int rFailure = 0;
  constexpr int rFailureGetLock = -2;
  constexpr int rNoEmptySlot = -3;
  constexpr int rSegmentChanged = -4;
  constexpr int rSplitSuccess = -5;
  constexpr int rPreSegment = -6;

  constexpr size_t kFingerBits = 8;
  constexpr size_t kMask = (1 << kFingerBits) - 1;

  constexpr size_t kNumBucket = 64; /* the number of normal buckets in one segment*/
  constexpr size_t kSparePairNum = 256; /* the number of stash buckets in one segment*/
  constexpr uint16_t kBucketNormalSlotNum = 6;
  constexpr uint16_t kBucketSpareSlotNum = 10;
  constexpr uint16_t kBucketTotalSlotNum = kBucketNormalSlotNum + kBucketSpareSlotNum;
  constexpr uint16_t SlotMask = 15;
  constexpr uint16_t kBucketFull = (uint16_t) - 1;

  constexpr size_t bucketMask = kNumBucket - 1;
  constexpr size_t kSplitNum = 2;
  constexpr size_t kSplitMask = (1UL << kSplitNum) - 1;

  std::atomic<uint64_t> count = {0};

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
#define KEY_FINGER(hash) ((hash)&kMask);
#define BUCKET_INDEX(hash) ((hash >> kFingerBits) & bucketMask)
#define NEXT_BUCKET(y) (((y) & 1) ? ((y) - 1) : ((y) + 1))

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

    inline PmOffset GetValue()
    {
      return value;
    }
    inline uint64_t GetHash()
    {
      return key;
    }
  } __attribute__((packed));

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

    inline void ClearBits(uint64_t invalid_bits)
    {
      b = b & (~invalid_bits);
    }
  } __attribute__((packed));

  template <class KEY, class VALUE>
  struct SpareBucket
  {
    Lock16 lock[4];    // 4B
    Bitmap bitmap[4];  // 32B
    _Pair _[kSparePairNum];

    static size_t GetSize() { return kSparePairNum; }

    inline void ClearBit(size_t pos)
    {
      bitmap[pos >> 6].UnsetBit(pos & 63);
    }

    SpareBucket()
    {
      for (size_t i = 0; i < 4; i++)
      {
        lock[i].Init();
        bitmap[i].Init();
      }
    }

    inline void Init() {
      for (size_t i = 0; i < 4; i++) {
        lock[i].Init();
        bitmap[i].Init();
      }
    }

    inline int Insert(uint64_t key, PmOffset value, uint8_t finger)
    {
      // s: loop traverse all stash slot
      auto k = finger & 3;
      for (size_t i = 0; i < 4; i++)
      {
        auto m = (k + i) & 3;
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

    bool Delete(Pair_t<KEY, VALUE>* p, size_t key_hash, size_t position,
                PmManage<KEY,VALUE>* pm) {
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
        return true;
        auto o = _[pos].value;
        auto v = reinterpret_cast<Pair_t<KEY, VALUE>*>(o.GetValue());
        if (v->str_key() == p->str_key()) {
          return rSuccess;
        }
      }
      return rFailure;
    }

    inline bool Get(Pair_t<KEY, VALUE>* p, uint64_t key_hash, uint64_t pos) {
      if (_[pos].key == key_hash)
      {
        // return true;
        auto o = _[pos].value;
        // auto addr = reinterpret_cast<char *>(o.chunk_start_addr + o.offset);
        // pv[GetNum].kp.push_back(addr);
        // _mm_prefetch(addr,_MM_HINT_NTA);
        auto k = reinterpret_cast<const __m128i *>(o.GetValue());
        auto m = _mm_loadu_si128(k);
        auto v = reinterpret_cast<Pair_t<KEY,VALUE>*>(&m);
        // auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.chunk_start_addr + o.offset);
        // if(v!=nullptr) return true;
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
  } __attribute__((packed));

  template <class KEY, class VALUE>
  struct Bucket
  {
    VersionLock16 lock;      // 2B
    uint16_t bitmap;       // 2B
    uint8_t fingers[kBucketTotalSlotNum];
    uint8_t s_pos[kBucketSpareSlotNum];
    uint8_t pad[2];
    _Pair slot[kBucketNormalSlotNum];

    Bucket() : bitmap(0) { lock.Init(); }

    inline void Init()
    {
      lock.Init();
      bitmap = 0;
    }

    inline void BkLoadFactor(bool before_split)
    {
      auto c = __builtin_popcount(bitmap);
      auto f = (float)c / (float)kBucketTotalSlotNum;
      if (before_split)
        printf("b: bk load factor: %f\n", f);
      else
        printf("a: bk load factor: %f\n", f);
    }

    inline int GetCount() { return __builtin_popcount(bitmap); }

    static size_t GetSlotNum() { return kBucketNormalSlotNum; }

    bool FindDuplicate(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s: get possible slots by finger
      auto mask = CMP128(fingers, finger);
      // s: filter invalid slot
      mask = mask & bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum) {
            // s: search in normal bucket
            if (slot[pos].key == key_hash) {
              auto o = slot[pos].value;
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key()) { return rSuccess; }
            }
          } else {
            // s: search in spare bucket
            auto r = sb->FindDuplicate(p, key_hash, s_pos[pos - kBucketNormalSlotNum]);
            if (rSuccess == r){ return rSuccess; }
          }
          mask &= (~(1 << pos));
        } while (mask);
      }
      return rFailure;
    }

    bool Update(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
                PmManage<KEY, VALUE> *pm, size_t thread_id, SpareBucket<KEY, VALUE> *sb)
    {
      // s: get possible slots by finger
      auto mask = CMP128(fingers, finger);
      // s: filter invalid slot
      mask = mask & bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum)
          {
            // s: search in normal bucket
            if (slot[pos].key == key_hash)
            {
              auto o = slot[pos].value;
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key())
              {
                // s: update kv only if key is euqal
                slot[pos].value = pm->Update(p, thread_id, slot[pos].value);
                return rSuccess;
              }
            }
          }
          else
          {
            // s: update in spare bucket
            auto r = sb->Update(p, key_hash, thread_id, s_pos[pos - kBucketNormalSlotNum], pm);
            if (rSuccess == r)
              return rSuccess;
          }
          mask &= (~(1 << pos));
        } while (mask);
      }
      return rFailure;
    }

    bool Get(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s: get possible slots by finger
      auto mask = CMP128(fingers, finger);
      // s: filter invalid slot
      mask = mask & bitmap;
      if (mask) {
        // s: prefetch vector
        do {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum) {
            // s: search in normal bucket
            if (slot[pos].key == key_hash) {
              auto o = slot[pos].value;
              auto k = reinterpret_cast<const __m128i *>(o.GetValue());
              auto m = _mm_loadu_si128(k);
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(&m);
              // if (v != nullptr) return true;
              // auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.chunk_start_addr + o.offset);
              if (p->str_key() == v->str_key())
              {
                p->load(reinterpret_cast<char *>(v));
                return rSuccess;
              }
            }
          } else {
            // s: search in spare bucket
            auto r = sb->Get(p, key_hash, s_pos[pos - kBucketNormalSlotNum]);
            if (rSuccess == r) return rSuccess;
          }
          mask &= (~(1 << pos));
        } while (mask);
      }
      return rFailure;
    }

    /* set bitmap metadata */
    inline void SetMeta(uint16_t bucket_slot, uint8_t finger) {
      // s: set finger
      fingers[bucket_slot] = finger;
      // s: set bitmap
      bitmap = bitmap | (1 << bucket_slot);
    }

    inline void SetMetaWithStash(int bucket_slot, int s_slot,
                                 uint8_t finger) {
      // s1: set finger
      fingers[bucket_slot] = finger;
      // s2: set pos for kv in spare bucket
      s_pos[bucket_slot - kBucketNormalSlotNum] = s_slot;
      // s3: set bitmap
      bitmap = bitmap | (1 << bucket_slot);
    }

    inline void UnsetMeta(uint16_t index) {
      // s1: update bitmap
      bitmap &= (~(1 << index));
    }

    inline int FindEmptySlot(uint8_t finger) {
      if (kBucketFull == bitmap)
      {
        return rNoEmptySlot;
      }
      return  __builtin_ctz(~bitmap);
    }

    inline int Insert(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
                      PmManage<KEY, VALUE> *pm, size_t thread_id, SpareBucket<KEY, VALUE> *sb)
    {
      // s1: check whether exist duplicate key-value;
      if (FindDuplicate(p, key_hash, finger, sb))
        return rSuccess;
      // s2: insert key-value to pm
      if (tl_value == PO_NULL) tl_value = pm->Insert(p, thread_id);
      // s2: get empy slot if exist
      auto pos = FindEmptySlot(finger);
      if (rNoEmptySlot == pos) {
        return rNoEmptySlot;
      }
      // s3: insert kv to bucket or stash
      if (pos < kBucketNormalSlotNum) {
        // s3.1: insert into slot in current bucket
        slot[pos].value = tl_value;
        slot[pos].key = key_hash;
        SetMeta(pos, finger);
      } else {
        // s3.2: insert into slot in stash
        auto k = sb->Insert(key_hash, tl_value, finger);
        if (rNoEmptySlot != k) {
          SetMetaWithStash(pos, k, finger);
        } else {
          return rNoEmptySlot;
        }
      }
      return rSuccess;
    }

    inline int Insert(_Pair &p, uint8_t finger, SpareBucket<KEY, VALUE> *sb)
    {
      // s1: get empy slot if exist
      auto pos = FindEmptySlot(finger);
      if (pos == rNoEmptySlot) {
        return rNoEmptySlot;
      }
      // s2: insert kv to bucket or stash
      if (pos < kBucketNormalSlotNum) {
        // s3.1: insert into slot in current bucket
        slot[pos].value = p.value;
        slot[pos].key = p.key;
        SetMeta(pos, finger);
      } else {
        // s3.2: insert into slot in stash
        auto k = sb->Insert(p.key, p.value, finger);
        if (rNoEmptySlot != k) {
          SetMetaWithStash(pos, k, finger);
        } else {
          return rNoEmptySlot;
        }
      }
      return rSuccess;
    }

    /*if delete success, then return 0, else return -1*/
    int Delete(Pair_t<KEY, VALUE> *p, uint64_t key_hash, uint8_t finger,
               PmManage<KEY, VALUE> *pm, SpareBucket<KEY, VALUE> *sb)
    {
      // s: get possible slots by finger
      auto mask = CMP128(fingers, finger);
      // s: filter invalid slot
      mask = mask & bitmap;
      if (mask)
      {
        do
        {
          auto pos = __builtin_ctz(mask);
          if (pos < kBucketNormalSlotNum) {
            // s: search in normal bucket
            if (slot[pos].key == key_hash) {
              auto o = slot[pos].value;
              auto v = reinterpret_cast<Pair_t<KEY, VALUE> *>(o.GetValue());
              if (p->str_key() == v->str_key())
              {
                // s: delete kv only if key is euqal
                pm->Delete(slot[pos].value);
                UnsetMeta(pos);
                return rSuccess;
              }
            }
          } else {
            // s: search in spare bucket
            auto r = sb->Delete(p, key_hash,
                                s_pos[pos - kBucketNormalSlotNum], pm);
            if (rSuccess == r)
            {
              UnsetMeta(pos);
              return rSuccess;
            }
          }
          mask &= (~(1 << pos));
        } while (mask);
      }
      return rFailure;
    }
  } __attribute__((packed));

}