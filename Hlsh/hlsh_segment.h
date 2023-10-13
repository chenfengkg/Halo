#include "hlsh_bucket.h"
namespace HLSH_hashing
{
  template <class KEY, class VALUE>
  struct Directory;
  template <class KEY, class VALUE>
  struct SegmentAllocator;

  enum class SplitState : uint8_t
  {
    NewSegmentNoSplit,   // 0
    NewSegmentSplit,     // 1
    SplitSegmentNoSplit, // 2
    SplitSegmentSplit,   // 3
  };

  template<class KEY,class VALUE>
  struct PersistHash;

  /* the segment class*/
  template <class KEY, class VALUE>
  struct Segment
  {
    Lock64 lock;        // 8B: lock for directory entries
    PersistHash<KEY,VALUE>* ph; //8B
    uint64_t pattern : 58; // 8B
    uint64_t local_depth : 6;
    uint64_t pad[5];
    SpareBucket<KEY, VALUE> sbucket;
    Bucket<KEY, VALUE> bucket[kNumBucket];


    static size_t GetSlotNum() {
      return kNumBucket * Bucket<KEY, VALUE>::GetSize() +
             SpareBucket<KEY, VALUE>::GetSize();
    }

    inline void Init()
    {
      // s: init lock
      lock.Init();
      // s: init persist hash
      ph = nullptr;
      // s: init share buckets
      sbucket.Init();
      // s: init bucket
      for (size_t i = 0; i < kNumBucket; i++)
      {
        auto b = bucket + i;
        b->Init();
      }
    }

    inline void Recovery(){
      // s: segment lock
      lock.Init();
      // s: persist hash
      ph = nullptr;
      // s: spare buckets lock
      for (size_t i = 0; i < kSpareBucketNum; i++)
      {
        sbucket.lock[i].Init();
      }
      // s: buckets lock
      for (size_t i = 0; i < kNumBucket; i++)
      {
        auto b = bucket + i;
        b->bf.lock.Init();
      }
    }

    static void New(Segment<KEY, VALUE> **tbl, size_t depth, size_t pattern)
    {
      // s1: allocate new segment by memory allocator
      auto r = posix_memalign(reinterpret_cast<void **>(tbl), kCacheLineSize, sizeof(Segment<KEY, VALUE>));
      if (!r)
      {
        auto t = *tbl; 
        // s: initialization
        t->Init();
        // s: update local depth and pattern
        t->local_depth = depth;
        t->pattern = pattern;
      }
      else
      {
        printf("Allocate segment failure\n");
        fflush(stdout);
      }
    }
    

    Segment() { }

    ~Segment(void) {}

    void Check();
    inline int Insert(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                      HLSH<KEY, VALUE> *index, PmManage<KEY, VALUE> *pm,
                      size_t thread_id);
    inline int Delete(Pair_t<KEY, VALUE> *p, size_t key_hash,
                      PmManage<KEY, VALUE> *pm, HLSH<KEY, VALUE> *index);
    inline bool Get(Pair_t<KEY, VALUE> *p, size_t key_hash, int &extra_rcode);
    inline int Update(Pair_t<KEY, VALUE> *p, size_t key_hash,
                      PmManage<KEY, VALUE> *pm, size_t thread_id,
                      HLSH<KEY, VALUE> *index);
    inline int UpdateForReclaim(Pair_t<KEY, VALUE> *p, size_t key_hash,
                                PmOffset old_value, PmOffset new_value,
                                PmManage<KEY, VALUE> *pm, HLSH<KEY, VALUE> *index);
    inline int HelpSplit();
    inline int SplitBucket(Bucket<KEY, VALUE> *, Segment<KEY, VALUE> *, size_t);
    inline void GetBucketsLock();
    inline void ReleaseBucketsLock();
    Segment<KEY, VALUE> *Split();


    void SegLoadFactor()
    {
      // s1: calculate the total valid number in segment
      float count = 0;
      for (size_t i = 0; i < kNumBucket; i++)
      {
        Bucket<KEY, VALUE> *curr_bucket = bucket + i;
        count += curr_bucket->GetCount();
      }
      // s2: calculate load factor
      auto lf = count / (kBucketNormalSlotNum * kNumBucket + kSparePairNum);
      printf("this: %p, pattern: %lu, local_depth: %lu, lf: %f\n", this,
             pattern, local_depth, lf);
    }
  } PACKED;

  template <class KEY, class VALUE>
  void Segment<KEY, VALUE>::GetBucketsLock()
  {
    // s: get all buckets lock
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      b->bf.lock.GetLock();
    }
  }

  template <class KEY, class VALUE>
  void Segment<KEY, VALUE>::ReleaseBucketsLock()
  {
    // s1: Release all buckets lock
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      b->bf.lock.ReleaseLock();
    }
  }

  template <class KEY, class VALUE>
  void Segment<KEY, VALUE>::Check()
  {
    int sumBucket = kNumBucket;
    size_t total_num = 0;
    for (size_t i = 0; i < sumBucket; i++)
    {
      auto curr_bucket = bucket + i;
      int num = __builtin_popcount(GET_BITMAP(curr_bucket->bitmap));
      total_num += num;
      if (i < (sumBucket - 1))
        printf("%d ", num);
      else
        printf("%d\n", num);
    }
    float lf = (float)total_num / Segment<KEY, VALUE>::GetSlotNum();
    printf("load_factor: %f\n", lf);
  }

  /* it needs to verify whether this bucket has been deleted...*/
  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::Insert(Pair_t<KEY, VALUE> *p, uint64_t key_hash,
                                  HLSH<KEY, VALUE> *index,
                                  PmManage<KEY, VALUE> *pm, size_t thread_id)
  {
    // s: get finger for key and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);
  RETRY:
    // s: obtain lock for target bucket
    Bucket<KEY, VALUE> *t = bucket + y;
    // s: get lock
    t->bf.lock.GetLock();
    // s: judge whether segment has been split
    if (this != index->GetSegment(key_hash))
    {
      t->bf.lock.ReleaseLock();
      return rSegmentChanged;
    }
    // s: insert to target bucket
    auto r = t->Insert(p, key_hash, finger, pm, thread_id, &sbucket);
    // s: release lock
    t->bf.lock.ReleaseLock();

    return r;
  }

  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::Delete(Pair_t<KEY, VALUE> *p, size_t key_hash,
                                  PmManage<KEY, VALUE> *pm,
                                  HLSH<KEY, VALUE> *index)
  {
    // s0: get finger for key and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);
    // s1: get bucket lock
    auto t = bucket + y;
  RETRY:
    t->bf.lock.GetLock();
    // s: check wheter segment has been splited
    if (this != index->GetSegment(key_hash))
    {
      t->bf.lock.ReleaseLock();
      return rSegmentChanged;
    }
    // s: delete
    auto r = t->Delete(p, key_hash, finger, pm, &sbucket);
    // s: release bucket lock
    t->bf.lock.ReleaseLock();

    return r;
  }

  /* it needs to verify whether this bucket has been deleted...*/
  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::Update(Pair_t<KEY, VALUE> *p, size_t key_hash,
                                  PmManage<KEY, VALUE> *pm, size_t thread_id,
                                  HLSH<KEY, VALUE> *index)
  {
    // s0: get finger for key and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);
    // s2: get bucket lock
    auto t = bucket + y;
  RETRY:
    t->bf.lock.GetLock();
    // s4: update
    auto r = t->Update(p, key_hash, finger, pm, thread_id, &sbucket);
    if (rFailure == r)
    {
      if (this != index->GetSegment(key_hash))
      {
        t->bf.lock.ReleaseLock();
        return rSegmentChanged;
      }
    }
    t->bf.lock.ReleaseLock();

    return r;
  }

  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::UpdateForReclaim(
      Pair_t<KEY, VALUE> *p, size_t key_hash,
      PmOffset old_value, PmOffset new_value,
      PmManage<KEY, VALUE> *pm, HLSH<KEY, VALUE> *index)
  {
    // s0: get finger for key and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);
    // s2: get bucket lock
    auto t = bucket + y;
  RETRY:
    t->bf.lock.GetLock();
    // s4: update
    auto r = t->UpdateForReclaim(p, key_hash,
                                 old_value, new_value,
                                 finger, pm, &sbucket);
    if (rFailure == r)
    {
      if (this != index->GetSegment(key_hash))
      {
        t->bf.lock.ReleaseLock();
        return rSegmentChanged;
      }
    }
    t->bf.lock.ReleaseLock();

    return r;
  }

  template <class KEY, class VALUE>
  bool Segment<KEY, VALUE>::Get(Pair_t<KEY, VALUE> *p, size_t key_hash,
                                int &extra_rcode)
  {
    // s0: get finger and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);

  RETRY:
    // s1: obtain pointer for target
    auto target = bucket + y;
    // s2: get version
    auto old_version = target->bf.lock.GetVersionWithoutLock();
    // s3: get and return value from target bucket if value exist
    auto r = target->Get(p, key_hash, finger, &sbucket);
    // s4: retry or return based on return value
    if (rSuccess != r)
    {
      //  s4.1: retry if version change or return
      if (target->bf.lock.LockVersionIsChanged(old_version))
      {
        goto RETRY;
      }
    }
    return r;
  }


  /* Split target buckets*/
  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::SplitBucket(Bucket<KEY, VALUE> *split_bucket,
                                       Segment<KEY, VALUE> *new_seg,
                                       size_t bucket_index)
  {
    Segment<KEY, VALUE> *split_seg = this;
    auto new_bucket = new_seg->bucket + bucket_index;

    // s: move value in normal bucket
    uint16_t invalid_mask = 0;
    for (int j = 0; j < kBucketNormalSlotNum; ++j)
    {
      if (CHECK_BIT(split_bucket->bf.bitmap, j))
      {
        auto key_hash = split_bucket->bs.key[j];
        if ((key_hash >> (64 - new_seg->local_depth)) == new_seg->pattern)
        {
          SET_BIT16(invalid_mask, j);
          new_bucket->Insert(split_bucket->bs.key[j], split_bucket->bf.value[j],
                             split_bucket->bf.fingers[j], &new_seg->sbucket);
        }
      }
    }
    // s: move value in spare bucket
    auto split_sbucket = &sbucket;
    for (size_t j = 0; j < kBucketNormalSlotNum; j++)
    {
      if (CHECK_BIT(split_bucket->bf.bitmap, j + kBucketNormalSlotNum))
      {
        auto pos = split_bucket->bf.value[j].spos;
        auto key_hash = split_sbucket->_[pos].key;
        if ((key_hash >> (64 - new_seg->local_depth)) == new_seg->pattern)
        {
          SET_BIT16(invalid_mask, j + kBucketNormalSlotNum);
          new_bucket->Insert(split_sbucket->_[pos].key, split_sbucket->_[pos].value,
                             split_bucket->bf.fingers[j + kBucketNormalSlotNum],
                             &new_seg->sbucket);
          split_sbucket->ClearBit(pos);
        }
      }
    }
    UNSET_BITS(split_bucket->bf.bitmap, invalid_mask);
    // s: move value in spare bucket
    if (CHECK_BIT(split_bucket->bf.bitmap, 12))
    {
      uint8_t invalid_mask8 = 0;
      for (size_t j = 0; j < 7; j++)
      {
        if (CHECK_BIT(split_bucket->bs.bitmap, j))
        {
          auto pos = split_bucket->bs.spos[j].spos;
          auto key_hash = split_sbucket->_[pos].key;
          if ((key_hash >> (64 - new_seg->local_depth)) == new_seg->pattern)
          {
            SET_BIT8(invalid_mask8, j);
            new_bucket->Insert(split_sbucket->_[pos].key, split_sbucket->_[pos].value,
                               split_bucket->bs.spos[j].finger, &new_seg->sbucket);
            split_sbucket->ClearBit(pos);
          }
        }
      }
      UNSET_BITS(split_bucket->bs.bitmap, invalid_mask8);
      if (!split_bucket->bs.bitmap)
      {
        UNSET_BIT16(split_bucket->bf.bitmap, 12);
      }
    }
    return rSuccess;
  }

  template <class KEY, class VALUE>
  Segment<KEY, VALUE> *Segment<KEY, VALUE>::Split()
  {
    // s1: update medata in split segment
    local_depth = local_depth + 1;
    pattern = pattern << 1;
    // s2: allocate new segment and update metadata in new segment
    size_t new_pattern = pattern + 1;
    Segment<KEY,VALUE>* new_seg;
    Segment<KEY, VALUE>::New(&new_seg, local_depth, new_pattern);
    // s4: get lock for new seg
    new_seg->lock.GetLock();
    new_seg->GetBucketsLock();

    // s5: split all bucket
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      SplitBucket(b, new_seg, i);
    }
    return new_seg;
  }
} // namespace HLSH_hashing