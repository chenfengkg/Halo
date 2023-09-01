#include "hlsh_bucket.h"
namespace HLSH_hashing {
template <class KEY, class VALUE>
struct Directory;
template <class KEY, class VALUE>
struct SegmentAllocator;

enum class SplitState : uint8_t {
  NewSegmentNoSplit,    // 0
  NewSegmentSplit,      // 1
  SplitSegmentNoSplit,  // 2
  SplitSegmentSplit,    // 3
};

/* the segment class*/
template <class KEY, class VALUE>
struct Segment {
  VersionLock lock;           // 8B: lock for directory entries
  uint64_t pattern: 58;       //8B
  uint64_t local_depth: 6;
  Segment<KEY, VALUE>* next;  // 8B
  Segment<KEY, VALUE>* pre;   // 8B
  Stash<KEY, VALUE> stash;
  Bucket<KEY, VALUE> bucket[kNumBucket];

  static size_t GetSlotNum() {
    return kNumBucket * Bucket<KEY, VALUE>::GetSize() +
           Stash<KEY, VALUE>::GetSize();
  }

  static void New(Segment<KEY, VALUE>** tbl, size_t depth,
                  Segment<KEY, VALUE>* p_next, size_t pattern) {
    // s1: allocate new segment by memory allocator
    if (!posix_memalign(reinterpret_cast<void**>(tbl), kCacheLineSize,
                        sizeof(Segment<KEY, VALUE>))) {
      // s2: link to segment list
      auto t = *tbl;
      t->next = p_next;
      if (p_next) p_next->pre = t;
      // s3: update local depth
      t->local_depth = depth;
      t->pattern = pattern;
      for(size_t i = 0;i<kNumBucket;i++){
        auto b = t->bucket+i;
        b->stash = &t->stash;
      }
    }
  }

  static void New(Segment<KEY, VALUE>** tbl, size_t depth, size_t pattern,
                  Segment<KEY, VALUE>* p_next,
                  SegmentAllocator<KEY, VALUE>* salloc, size_t thread_id) {
#ifdef ENABLE_PREALLOC
    // s1: allocate new segment form preallocated memory block
    (*tbl) = salloc->Get(thread_id);
    // s2: link to segment list
    auto t = *tbl;
    t->next = p_next;
    if (p_next) p_next->pre = t;
    // s3: update local depth
    t->local_depth = depth;
    t->pattern = pattern;

    AddSlots(GetSlotNum());
#else
    New(tbl, depth, p_next, pattern);
#endif
  }

  Segment() {
    // set stash value to bucket
    for (size_t i = 0; i < kNumBucket; i++) {
      bucket[i].stash = &stash;
    }
  }

  ~Segment(void) {}

  void Check();
  inline int Insert(Pair_t<KEY, VALUE>* p, uint64_t key_hash,
                    HLSH<KEY, VALUE>* index, PmManage<KEY, VALUE>* pm,
                    size_t thread_id);
  inline int Delete(Pair_t<KEY, VALUE>* p, size_t key_hash,
                    PmManage<KEY, VALUE>* pm, HLSH<KEY, VALUE>* index);
  inline bool Get(Pair_t<KEY, VALUE>* p, size_t key_hash, int& extra_rcode);
  inline int Update(Pair_t<KEY, VALUE>* p, size_t key_hash,
                    PmManage<KEY, VALUE>* pm, size_t thread_id,
                    HLSH<KEY, VALUE>* index);
  inline int HelpSplit();
  inline int SplitBucket(Bucket<KEY, VALUE>*, size_t);
  inline void SplitRemainBuckets();
  inline void GetLockForSplit(bool is_split);
  inline void ReleaseLockForSplit();
  Segment<KEY, VALUE>* Split(SegmentAllocator<KEY, VALUE>*, size_t);

  /*InitalSeg: split seg*/
  inline void InitSeg() {
    for (size_t i = 0; i < kNumBucket; i++) {
      auto b = bucket + i;
      b->lock.InitBucket();
    }
  }

  void SegLoadFactor() {
    // s1: calculate the total valid number in segment
    float count = 0;
    for (size_t i = 0; i < kNumBucket; i++) {
      Bucket<KEY, VALUE>* curr_bucket = bucket + i;
      count += curr_bucket->GetCount();
    }
    // s2: calculate load factor
    auto lf = count / (kBucketNormalSlotNum * kNumBucket + 64);
    printf("this: %p, pattern: %lu, local_depth: %lu, lf: %f\n", this,
           pattern, local_depth, lf);
  }
};

template <class KEY, class VALUE>
void Segment<KEY, VALUE>::GetLockForSplit(bool is_split) {
  // s2: get all buckets lock
  for (size_t i = 0; i < kNumBucket; i++) {
    auto b = bucket + i;
    b->lock.GetLockForSplit(is_split);
  }
}

template <class KEY, class VALUE>
void Segment<KEY, VALUE>::ReleaseLockForSplit() {
  // s1: Release all buckets lock
  for (size_t i = 0; i < kNumBucket; i++) {
    auto b = bucket + i;
    // s1.1: release lock with buckets are not split
    // b->lock.ReleaseLock();
    b->lock.ReleaseLockForSplit();
  }
  // s2: Release segment lock
  lock.ReleaseLock();
}

template <class KEY, class VALUE>
void Segment<KEY, VALUE>::Check() {
  int sumBucket = kNumBucket;
  size_t total_num = 0;
  for (size_t i = 0; i < sumBucket; i++) {
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
int Segment<KEY, VALUE>::Insert(Pair_t<KEY, VALUE>* p, uint64_t key_hash,
                                HLSH<KEY, VALUE>* index,
                                PmManage<KEY, VALUE>* pm, size_t thread_id) {
  // s0: get finger for key and bucket index
  auto finger = KEY_FINGER(key_hash);  // the last 8 bits
  auto y = BUCKET_INDEX(key_hash);
RETRY:
  // s1: obtain lock for target bucket
  Bucket<KEY, VALUE>* t = bucket + y;
  t->lock.GetLock();
  // s1: help to split bucket
  // auto sr = SplitBucket(t, y);
  // if (rFailureGetLock == sr) { t->lock.ReleaseLock(); goto RETRY; }
  bool is_split = false;
  // if (rSplitSuccess == sr) { is_split = true; }
  // s2: if segment is changed, need to reinsert
  if (this != index->GetSegment(key_hash)) {
    t->lock.ReleaseLock();
    return rSegmentChanged;
  }
  // s3: insert to target bucket
  auto r = t->Insert(p, key_hash, finger, pm, thread_id);
  // s4: release bucket lock
  if (is_split)
    t->lock.ReleaseLockForSplit();
  else
    t->lock.ReleaseLock();

  return r;
}

template <class KEY, class VALUE>
int Segment<KEY, VALUE>::Delete(Pair_t<KEY, VALUE>* p, size_t key_hash,
                                PmManage<KEY, VALUE>* pm,
                                HLSH<KEY, VALUE>* index) {
  // s0: get finger for key and bucket index
  auto finger = KEY_FINGER(key_hash);  // the last 8 bits
  auto y = BUCKET_INDEX(key_hash);
  // s1: get bucket lock
  auto t = bucket + y;
RETRY:
  t->lock.GetLock();
  // s2: help slit bucket
  auto sr = SplitBucket(t, y);
  if (rFailureGetLock == sr) {
    t->lock.ReleaseLock();
    goto RETRY;
  }
  bool is_split = false;
  if (rSplitSuccess == sr) {
    is_split = true;
  }
  // s3: delete
  auto r = t->Delete(p, key_hash, finger, pm);
  if (rFailure == r) {
    if (this != index->GetSegment(key_hash)) {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
  }
  // s4: release bucket lock
  if (is_split)
    t->lock.ReleaseLockForSplit();
  else
    t->lock.ReleaseLock();

  return r;
}

/* it needs to verify whether this bucket has been deleted...*/
template <class KEY, class VALUE>
int Segment<KEY, VALUE>::Update(Pair_t<KEY, VALUE>* p, size_t key_hash,
                                PmManage<KEY, VALUE>* pm, size_t thread_id,
                                HLSH<KEY, VALUE>* index) {
  // s0: get finger for key and bucket index
  auto finger = KEY_FINGER(key_hash);  // the last 8 bits
  auto y = BUCKET_INDEX(key_hash);
  // s2: get bucket lock
  auto t = bucket + y;
RETRY:
  t->lock.GetLock();
  // s3: help split
  auto sr = SplitBucket(t, y);
  if (rFailureGetLock == sr) {
    t->lock.ReleaseLock();
    goto RETRY;
  }
  bool is_split = false;
  if (rSplitSuccess == sr) {
    is_split = true;
  }
  // s4: update
  auto r = t->Update(p, key_hash, finger, pm, thread_id);
  if (rFailure == r) {
    if (this != index->GetSegment(key_hash)) {
      t->lock.ReleaseLock();
      return rSegmentChanged;
    }
  }
  // s4: release bucket lock
  if (is_split)
    t->lock.ReleaseLockForSplit();
  else
    t->lock.ReleaseLock();

  return r;
}

template <class KEY, class VALUE>
bool Segment<KEY, VALUE>::Get(Pair_t<KEY, VALUE>* p, size_t key_hash,
                              int& extra_rcode) {
  // s0: get finger and bucket index
  auto finger = KEY_FINGER(key_hash);  // the last 8 bits
  auto y = BUCKET_INDEX(key_hash);

RETRY:
  // s1: obtain pointer for target
  auto target = bucket + y;
  // s2: get version
  auto old_version = target->lock.GetVersionWithoutLock();
  // pv.ov = old_version;
  // s3: get and return value from target bucket if value exist
  auto r = target->Get(p, key_hash, finger);
  // s4: retry or return based on return value
  if (rSuccess != r) {
    //  s4.1: retry if version change or return
    if (target->lock.LockVersionIsChanged(old_version)) {
      goto RETRY;
    }
  } else {
    // s4.2: two case need to retry: 1: segment is changed 2. bucket is not
    // split
    auto old_value = __atomic_load_n(&target->lock.vlock.pad, __ATOMIC_ACQUIRE);
    BucketVersionLock l(old_value);
    if (l.vlock.is_split) {
      // s4.2.1: need to check whether other segment exist valid only if bucket
      // split
      extra_rcode = rSegmentChanged;
    } else if (!l.vlock.is_split_seg) {
      // s4.2.1: value may exist previous segment
      extra_rcode = rPreSegment;
    }
  }
  return r;
}

template <class KEY, class VALUE>
void Segment<KEY, VALUE>::SplitRemainBuckets() {
  // s: traverse and split bucket
  for (size_t i = 0; i < kNumBucket; i++) {
    auto b = bucket + i;
  RETRY:
    b->lock.GetLock();
    auto r = SplitBucket(b, i);
    if (rFailureGetLock == r) {
      b->lock.ReleaseLock();
      goto RETRY;
    }
    b->lock.ReleaseLockForSplit();
  }
}

/* Split target buckets*/
template <class KEY, class VALUE>
int Segment<KEY, VALUE>::SplitBucket(Bucket<KEY, VALUE>* target,
                                     size_t bucket_index) {
  // s1: check whether bucket is split
  // s1.1: get vlock
  uint16_t old_value =
      __atomic_load_n(&target->lock.vlock.pad, __ATOMIC_ACQUIRE);
  // s1.2: check wheteher lock is set
  BucketVersionLock l(old_value);
  if (l.vlock.is_split) return rSuccess;

  // s2: get split target and new seg pointer
  Segment<KEY, VALUE>*split_seg, *new_seg, *lock_seg;
  if (l.vlock.is_split_seg) {
    split_seg = this;
    new_seg = next;
    lock_seg = next;
  } else {
    split_seg = pre;
    new_seg = this;
    lock_seg = pre;
  }

  // s3: get lock for bucket in other segment
  auto lock_bucket = lock_seg->bucket + bucket_index;
  // if (!lock_bucket->lock.TryGetLock()) return rFailureGetLock;
  /* s4: traverse bucket and move valid records to new segment*/
  // s4.1: determin split bucket and new bucket
  Bucket<KEY, VALUE>*split_bucket, *new_bucket;
  if (l.vlock.is_split_seg) {
    split_bucket = target;
    new_bucket = lock_bucket;
  } else {
    split_bucket = lock_bucket;
    new_bucket = target;
  }
  // split_bucket->BkLoadFactor(true);
  // s4.1: move value in slot
  uint32_t invalid_mask = 0;
  for (int j = 0; j < kBucketNormalSlotNum; ++j) {
    if (CHECK_BIT(split_bucket->bitmap, j)) {
      auto key_hash = split_bucket->slot[j].key;
      if ((key_hash >> (64 - new_seg->local_depth)) == new_seg->pattern) {
        invalid_mask = invalid_mask | ((uint32_t)1 << j);
        new_bucket->Insert(split_bucket->slot[j].key,
                           split_bucket->slot[j].value,
                           split_bucket->fingers[j]);
      }
    }
  }
  // s4.1 move value in stash
  uint64_t invalid_mask_stash = 0;
  for (size_t j = kBucketNormalSlotNum; j < kBucketTotalSlotNum; j++) {
    if (CHECK_BIT(split_bucket->bitmap, j)) {
      auto pos = split_bucket->stash_pos[j - kBucketNormalSlotNum];
      auto key_hash = split_bucket->stash->_[pos].key;
      if ((key_hash >> (64 - new_seg->local_depth)) == new_seg->pattern) {
        invalid_mask = invalid_mask | ((uint32_t)1 << j);
        invalid_mask_stash = invalid_mask_stash | (1UL << pos);
        new_bucket->Insert(split_bucket->stash->_[pos].key,
                           split_bucket->stash->_[pos].value,
                           split_bucket->fingers[j]);
      }
    }
  }

  // s5: clear bucket and stash metadata
  split_bucket->bitmap = split_bucket->bitmap & (~invalid_mask);
  split_bucket->stash->lock.GetLock();
  split_bucket->stash->bitmap.ClearBits(invalid_mask_stash);
  split_bucket->stash->lock.ReleaseLock();

  // split_bucket->BkLoadFactor(false);
  // new_bucket->BkLoadFactor(false);
  // s6: release bucket lock in other segment
  // lock_bucket->lock.ReleaseLockForSplit();
  return rSuccess;
}

/* split stash */
template <class KEY, class VALUE>
Segment<KEY, VALUE>* Segment<KEY, VALUE>::Split(
    SegmentAllocator<KEY, VALUE>* salloc, size_t thread_id) {
  // s1: update medata in split segment
  local_depth = local_depth + 1;
  pattern = pattern << 1;
  // s2: allocate new segment and update metadata in new segment
  size_t new_pattern = pattern + 1;
  Segment<KEY, VALUE>::New(&next, local_depth, new_pattern, next, salloc,
                           thread_id);
  // s3: and add into segment lists
  next->pre = this;
  // s4: get lock for split
  next->lock.GetLock();
  next->GetLockForSplit(false);

  // s5: split all bucket
  for (size_t i = 0; i < kNumBucket; i++) {
    auto b = bucket + i;
    SplitBucket(b, i);
  }
  return next;
}
}  // namespace HLSH_hashing