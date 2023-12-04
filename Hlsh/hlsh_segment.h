#include "hlsh_preallocate.h"
namespace HLSH_hashing
{
  template <class KEY, class VALUE>
  struct Directory;
  template<class KEY,class VALUE>
  struct PersistHash;

  std::atomic<uint64_t> segment_num{0};

  /* the segment class*/
  template <class KEY, class VALUE>
  struct Segment
  {
    Lock64 lock;        // 8B: lock for directory entries
    PersistHash<KEY,VALUE>* ph; //8B
    uint64_t pattern : 58; // 8B
    uint64_t local_depth : 6;
    uint64_t pad[5]; //40B
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
        b->lock.Init();
      }
    }

    static void New(Segment<KEY, VALUE> **tbl, size_t depth, size_t pattern)
    {
      if (!tl_schunk)
      {
        tl_schunk = reinterpret_cast<void *>(new SegChunkList<KEY, VALUE>());
      }
      auto s = reinterpret_cast<SegChunkList<KEY, VALUE> *>(tl_schunk)->Get();
      if (s)
      {
        s->local_depth = depth;
        s->pattern = pattern;
        *tbl = s;
      }
    }

    Segment()
    {
      ph = nullptr;
    }

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
    inline int SplitBucket(Bucket<KEY, VALUE> *, Segment<KEY, VALUE> *, size_t, size_t);
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
  } ALIGNED(64);

  template <class KEY, class VALUE>
  void Segment<KEY, VALUE>::GetBucketsLock()
  {
    // s: get all buckets lock
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      b->lock.GetLock();
    }
  }

  template <class KEY, class VALUE>
  void Segment<KEY, VALUE>::ReleaseBucketsLock()
  {
    // s1: Release all buckets lock
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      b->lock.ReleaseLock();
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
                                  PmManage<KEY, VALUE> *pm,
                                  size_t thread_id)
  {
    // s: get finger for key and bucket index
    auto finger = KEY_FINGER(key_hash); // the last 8 bits
    auto y = BUCKET_INDEX(key_hash);
  RETRY:
    // s: obtain lock for target bucket
    Bucket<KEY, VALUE> *t = bucket + y;
    // s: lock version
    auto old_version = t->lock.GetVersion();
    // s1: check whether exist duplicate key-value;
    auto pos = t->FindDuplicate(p, key_hash, finger, &sbucket);
    if (pos != rFailure)
    {
      // s: duplicate key
      if (t->lock.VersionIsChanged(old_version))
      {
        goto RETRY;
      }
      return rSuccess;
    }
    // s: get lock
    t->lock.GetLock();
    if (t->lock.VersionIsChanged2(old_version))
    {
      t->lock.ReleaseLock();
      goto RETRY;
    }
    // s: judge whether segment has been split
    if (this != index->GetSegment(key_hash))
    {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
#ifndef DRAM_INDEX
    // s2: insert key-value to pm
    if (tl_value == PO_NULL)
    {
      tl_value = pm->Insert(p, thread_id);
    }
#endif
    // s: insert to target bucket
    auto r = t->Insert(key_hash, tl_value, finger, &sbucket);
    // s: release lock
    t->lock.ReleaseLock();

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
    // s: lock version
    auto old_version = t->lock.GetVersion();
    // s1: check whether exist duplicate key-value;
    auto pos = t->FindDuplicate(p, key_hash, finger, &sbucket);
    if (rFailure == pos)
    {
      // s: duplicate key
      if (t->lock.VersionIsChanged(old_version))
      {
        goto RETRY;
      }
      return rFailure;
    }
    // s: get lock
    t->lock.GetLock();
    if (t->lock.VersionIsChanged2(old_version))
    {
      if (!t->CheckKey(p, key_hash, &sbucket, pos))
      {
        t->lock.ReleaseLock();
        goto RETRY;
      }
    }
    // s: judge whether segment has been split
    if (this != index->GetSegment(key_hash))
    {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
    auto r = t->Delete(p, key_hash, finger, pm, &sbucket, pos);
    // s: release lock
    t->lock.ReleaseLock();
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
  RETRY:
    // s2: get bucket lock
    auto t = bucket + y;
    // s: lock version
    auto old_version = t->lock.GetVersion();
    // s1: check whether exist duplicate key-value;
    auto pos = t->FindDuplicate(p, key_hash, finger, &sbucket);
    if (rFailure == pos)
    {
      // s: duplicate key
      if (t->lock.VersionIsChanged(old_version))
      {
        goto RETRY;
      }
      return rFailure;
    }
    // s: get lock
    t->lock.GetLock();
    if (t->lock.VersionIsChanged2(old_version))
    {
      if (!t->CheckKey(p, key_hash, &sbucket, pos))
      {
        t->lock.ReleaseLock();
        goto RETRY;
      }
    }
    // s: judge whether segment has been split
    if (this != index->GetSegment(key_hash))
    {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
    // s: update
    auto r = t->Update(p, key_hash, pm, thread_id,
                       &sbucket, pos);
    // s: release lock
    t->lock.ReleaseLock();

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
    // s: lock version
    auto old_version = t->lock.GetVersion();
    // s1: check whether exist duplicate key-value;
    auto pos = t->FindDuplicate(p, key_hash, finger, &sbucket);
    if (rFailure == pos)
    {
      // s: duplicate key
      if (t->lock.VersionIsChanged(old_version))
      {
        goto RETRY;
      }
      return rFailure;
    }
    // s: get lock
    t->lock.GetLock();
    if (t->lock.VersionIsChanged2(old_version))
    {
      t->lock.ReleaseLock();
      goto RETRY;
    }
    // s: judge whether segment has been split
    if (this != index->GetSegment(key_hash))
    {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
    // s4: update
    auto r = t->UpdateForReclaim(p, key_hash, old_value, new_value,
                                 finger, pm, &sbucket, pos);
    // s: release lock
    t->lock.ReleaseLock();

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
    auto old_version = target->lock.GetVersion();
    // s3: get and return value from target bucket if value exist
    target->Get(p, key_hash, finger, &sbucket);
    //  s4.1: retry if version change or return
    if (target->lock.VersionIsChanged(old_version))
    {
      goto RETRY;
    }
    return true;
  }

  /* Split target buckets*/
  template <class KEY, class VALUE>
  int Segment<KEY, VALUE>::SplitBucket(Bucket<KEY, VALUE> *split_bucket,
                                       Segment<KEY, VALUE> *new_seg,
                                       size_t bucket_index,
                                       size_t new_pattern)
  {
    // s: new_seg range [1,(kSplitNum-1)]
    Bucket<KEY, VALUE> *nb = new_seg->bucket + bucket_index;
    auto sb = split_bucket;

    // s: move value in normal bucket
    uint16_t invalid_mask = 0;
    for (int j = 0; j < kBucketNormalSlotNum; ++j)
    {
      if (CHECK_BIT(sb->bitmap, j))
      {
        auto key_hash = sb->slot[j].hkey;
        if ((key_hash >> (64 - local_depth)) == new_pattern)
        {
          nb->Insert(sb->slot[j].hkey, sb->slot[j].value,
                     sb->fingers[j], &new_seg->sbucket);
          SET_BIT16(invalid_mask, j);
        }
      }
    }
    // s: move value in spare bucket
    auto ssb = &sbucket;
    for (size_t j = 0; j < kBucketNormalSlotNum; j++)
    {
      if (CHECK_BIT(sb->bitmap, j + kBucketNormalSlotNum))
      {
        auto pos = sb->slot[j].value.spos;
        if (((ssb->_[pos].hkey) >> (64 - local_depth)) == new_pattern)
        {
          nb->Insert(ssb->_[pos].hkey, ssb->_[pos].value,
                     sb->fingers[j + kBucketNormalSlotNum],
                     &new_seg->sbucket);
          SET_BIT16(invalid_mask, j + kBucketNormalSlotNum);
          ssb->ClearBit(pos);
        }
      }
    }
    UNSET_BITS(sb->bitmap, invalid_mask);
    // s: move value in spare bucket
    if (CHECK_BIT(sb->bitmap, 12))
    {
      uint8_t invalid_mask8 = 0;
      for (size_t j = 0; j < 7; j++)
      {
        if (CHECK_BIT(sb->sbitmap, j))
        {
          auto pos = sb->spos[j];
          if (((ssb->_[pos].hkey) >> (64 - local_depth)) == new_pattern)
          {
            nb->Insert(ssb->_[pos].hkey, ssb->_[pos].value,
                       sb->sfinger[j], &new_seg->sbucket);
            SET_BIT8(invalid_mask8, j);
            ssb->ClearBit(pos);
          }
        }
      }
      UNSET_BITS(sb->sbitmap, invalid_mask8);
      if (!sb->sbitmap)
      {
        UNSET_BIT16(sb->bitmap, 12);
      }
    }
    return rSuccess;
  }

  template <class KEY, class VALUE>
  Segment<KEY, VALUE> *Segment<KEY, VALUE>::Split()
  {
    // s1: update medata in current split segment
    auto old_local_depth = local_depth;
    local_depth = local_depth + 1;
    pattern = pattern << 1;
    // s2: allocate new segment and update metadata in new segment
    Segment<KEY, VALUE> *new_seg = nullptr;
    Segment<KEY, VALUE>::New(&new_seg, local_depth, pattern + 1);
    // s4: get lock for new seg
    new_seg->lock.GetLock();
    new_seg->GetBucketsLock();

    // s5: split all bucket
    for (size_t i = 0; i < kNumBucket; i++)
    {
      auto b = bucket + i;
      SplitBucket(b, new_seg, i, pattern + 1);
    }
    return new_seg;
  }
} // namespace HLSH_hashing