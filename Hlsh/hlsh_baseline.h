#include "hlsh_preallocated.h"

namespace HLSH_hashing {
std::atomic<uint64_t> insert_time;

template <class KEY, class VALUE>
class HLSH {
  Lock64 lock;  // the MSB is the lock bit; remaining bits are used as the counter
  Directory<KEY,VALUE>* dir;
  Directory<KEY,VALUE>* back_dir;
  PmManage<KEY,VALUE>* pm;
  bool clean;

 public:
  HLSH(const char*, const size_t&);
  HLSH(size_t, const char*, const size_t&);
  ~HLSH(void);
  inline bool Insert(Pair_t<KEY, VALUE>* p, size_t thread_id = 0,
                     bool is_recovery = false);
  inline bool Update(Pair_t<KEY, VALUE>* p, size_t thread_id = 0);
  inline bool Delete(Pair_t<KEY, VALUE>* p, size_t thread_id = 0);
  inline bool Get(Pair_t<KEY, VALUE>* p);
  inline void DirectoryDouble();
  inline void DirectoryUpdate(Directory<KEY, VALUE>* d,
                              Segment<KEY, VALUE>* new_seg);
  size_t GetDepth() { return dir->global_depth; }

  virtual void Recovery() { pm->Recovery(); }

  size_t get_capacity() { return this->num_segment * Segment<KEY,VALUE>::GetSlotNum(); }

  double get_load_factor() {
    uint64_t capcity = get_capacity();
    return (double)insert_time / (double)capcity;
  }

  void ShutDown() { clean = true; }

  void getNumber() {}

  /*get total capacity */
  size_t Capacity() {
    size_t _count = 0;
    size_t seg_count = 0;
    Directory<KEY,VALUE>* seg = dir;
    Segment<KEY,VALUE>** dir_entry = seg->_;
    Segment<KEY,VALUE>* ss;
    auto global_depth = seg->global_depth;
    size_t depth_diff;
    int capacity = pow(2, global_depth);
    for (int i = 0; i < capacity;) {
      ss = dir_entry[i];
      depth_diff = global_depth - ss->local_depth;
      _count += ss->number;
      seg_count++;
      i += pow(2, depth_diff);
    }
    return seg_count * Segment<KEY,VALUE>::GetSlotNum();
  }

  inline Segment<KEY,VALUE>* GetSegment(size_t key_hash) {
    // s1: get segment pointer from lastest dir
    auto d = __atomic_load_n(&dir, __ATOMIC_ACQUIRE);
    auto x = (key_hash >> (8 * sizeof(key_hash) - d->global_depth));
    // s2: get segment pointer from old dir if pointer is null
    auto seg = d->_[x];
    if (d->_[x] == nullptr) {
        seg = d->old_dir->_[x / 2];
    }
    return seg;
  }

  inline Segment<KEY,VALUE>* GetSegmentWithDirUpdate(
      size_t key_hash, Directory<KEY,VALUE>** copy_dir = nullptr) {
    // s1: get segment pointer from lastest dir
    auto d = __atomic_load_n(&dir, __ATOMIC_ACQUIRE);
    auto x = (key_hash >> (8 * sizeof(key_hash) - d->global_depth));
    Segment<KEY,VALUE>* seg = d->_[x];
    // s2: get segment from old dir if pointer is null
    if (!seg) {
      // s2.1: get valid segment from old dir
      auto old_dir = d->old_dir;
      seg = old_dir->_[x / 2];
      // s2.2: update entries to point to this valid segment
      seg->lock.GetLock();
      if (d->_[x]) {
        // s2.2.1: other thread has update this entry
        seg->lock.ReleaseLock();
        if (copy_dir) *copy_dir = d;
        return seg;
      }
      // s2.2.2: get start position and chunk size in new dir
      size_t start_pos = seg->pattern << (d->global_depth - seg->local_depth);
      size_t chunk_size = pow(2, d->global_depth - seg->local_depth);
      // s2.2.3: update dir entries point to this valid segment
      for (int i = chunk_size - 1; i >= 0; i--) {
        d->_[start_pos + i] = seg;
      }
      seg->lock.ReleaseLock();
    }
    if (copy_dir) *copy_dir = d;
    return seg;
  }
};

    template <class KEY, class VALUE>
    HLSH<KEY, VALUE>::HLSH(size_t initCap, const char* pool_file,
                           const size_t& pool_size) {
        // s2: init hash table on DRAM
        Directory<KEY,VALUE>::New(&back_dir, initCap, 0, nullptr);
        dir = back_dir;
        Segment<KEY,VALUE>* ptr = nullptr, * next = nullptr;
        for (int i = initCap - 1; i >= 0; --i) {
            Segment<KEY, VALUE>::New(&ptr, dir->global_depth, i);
            // s2.1: init seg as split segment 
            dir->_[i] = ptr;
        }
        // s: init PM management
        pm = new PmManage<KEY,VALUE>(pool_file, pool_size, this);
        // s: crash or normal shutdown
        clean = false;

        printf("size, segment: %lu, bucket: %lu\n", sizeof(Segment<KEY, VALUE>),
               sizeof(Bucket<KEY, VALUE>));
    }

    template <class KEY, class VALUE>
    HLSH<KEY, VALUE>::HLSH(const char* pool_file, const size_t& pool_size) {
        std::cout << "Reinitialize up" << std::endl;
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // s1: open PM
        pm = new PmManage<KEY,VALUE>(pool_file, pool_size, this);
        // s2: create a empty index on DRAM
        size_t depth = pm->GetDepth();
        Directory<KEY,VALUE>::New(&dir, pow(2, depth), 0);
        size_t initCap = pow(2, dir->global_depth);
        Segment<KEY,VALUE>* ptr = nullptr;
        for (int i = initCap - 1; i >= 0; --i) {
            Segment<KEY,VALUE>::New(&ptr, dir->global_depth, i);
            // s2.1: init seg as split segment
            ptr->SetSplitSeg();
            dir->_[i] = ptr;
        }
        // s3: insert key-value into index on DRAM
        Recovery();

        clock_gettime(CLOCK_MONOTONIC, &end);
        size_t elapsed = static_cast<size_t>((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
        float elapsed_sec = elapsed / 1000000000.0;
        printf("recovery time(LEH): %f s\n", elapsed_sec);
    }

    template <class KEY, class VALUE>
    HLSH<KEY, VALUE>::~HLSH(void) {
        size_t total_usage =  sizeof(PmManage<KEY, VALUE>) + sizeof(HLSH<KEY, VALUE>);
        float tu = total_usage;
        printf("HLSH DRAM total_Usage: %fB, %fKB, %fMB, %fGB\n", tu, tu / (1024.0), tu / (1024.0 * 1024.0), tu / (1024.0 * 1024.0 * 1024.0));
        printf("count: %lu\n", count.load());
    }

    template <class KEY, class VALUE>
    void HLSH<KEY, VALUE>::DirectoryUpdate(Directory<KEY, VALUE>* d,
                                           Segment<KEY, VALUE>* new_seg) {
        // s1: calculate start pos and range
        size_t depth_diff = d->global_depth - new_seg->local_depth;
        auto start_pos = new_seg->pattern << depth_diff;
        size_t chunk_size = pow(2, depth_diff);
        // s2: update entries
        for (int i = chunk_size - 1; i >= 0; i--) {
            d->_[start_pos + i] = new_seg;
        }
    }

    template <class KEY, class VALUE>
    void HLSH<KEY, VALUE>::DirectoryDouble() {
        // s1: persist new depth into PM
        auto global_depth = dir->global_depth;
        pm->SetDepth(global_depth + 1);
        // s: wait until old persist thread finish
        if (dir->ph) dir->ph->wait();
        // s: create new file for persisting index
        auto index_size = sizeof(PersistHash<KEY, VALUE>) + sizeof(uint64_t) * dir->capacity + sizeof(Segment<KEY, VALUE>) * dir->capacity;
        index_size = Round2StripeSize(index_size);
        uint64_t recovery_point[kThreadNum]; 
        pm->GetRecoveryPoint(recovery_point);
        auto persist_index = PersistHash<KEY, VALUE>::CreateNewPersist(dir, index_size, recovery_point);
        // s: allocate new directory
        auto capacity = pow(2, global_depth + 1);
        Directory<KEY,VALUE>::New(&back_dir, capacity, dir->version + 1, dir);
        back_dir->ph = persist_index;
        // s: traverse segments and set persist index
        for (size_t i = 0; i < dir->capacity;)
        {
            auto s = dir->_[i];
            s->lock.GetLock();
            s->ph = persist_index;
            size_t chunk_size = pow(2, dir->global_depth - s->local_depth);
            s->lock.ReleaseLock();
            i = i + chunk_size;
        }
        // s: set new dir to dir
        __atomic_store_n(&dir, back_dir, __ATOMIC_RELEASE);
        // s: start background thread to persist segment
        std::thread t(&PersistHash<KEY, VALUE>::BackPersistSegment, persist_index, dir, dir->old_dir);
        t.detach();
        // s: print directory
        printf("DirectoryDouble towards->%u\n", global_depth + 1);
    }

    /* insert key_value to dash */
    template <class KEY, class VALUE>
    bool HLSH<KEY,VALUE>::Insert(Pair_t<KEY, VALUE>* p, size_t thread_id,
                        bool is_recovery) {
        // s1: insert kv-pair to pm except recovery
        if (!is_recovery) {
            tl_value.InitValue();
        }

        // s2: caculate hash value for key-value pair
        uint64_t key_hash = h(p->key(), p->klen());

        /*s3: insert offset into hash map on DRAM*/
    RETRY:
        // s3.2: get segment pointer
        Directory<KEY,VALUE>* copy_dir = nullptr;
        auto seg = GetSegmentWithDirUpdate(key_hash, &copy_dir);
        // s3.3: insert to segment
        auto r = seg->Insert(p, key_hash, this, pm, thread_id);
        // s3.4: deal with according by return result
        if (rSuccess == r) { return 0; }
        if (rSegmentChanged == r) { goto RETRY; }
        // s3.4: segment need to split due to it is full
        if (rNoEmptySlot == r) {
            auto old_local_depth = seg->local_depth;
            if (old_local_depth < copy_dir->global_depth) {
                //s3.4.2: split segment without directory double
                seg->lock.GetLock();
                if (old_local_depth != seg->local_depth) {
                    // s3.4.2.1: other thread has split this segment
                    seg->lock.ReleaseLock();
                }
                else {
                    // s: persist segment to pm
                    if (nullptr != seg->ph) {
                        seg->ph->PersistSegment(seg);
                    }
                    // s3.4.2.2: split segment
                    auto d = __atomic_load_n(&dir, __ATOMIC_ACQUIRE);
                    if (d != copy_dir) {
                        // s3.4.2.2.1: dir double 
                        copy_dir = d;
                    }
                    seg->GetBucketsLock();
                    auto new_seg = seg->Split();
                    DirectoryUpdate(copy_dir, new_seg);
                    new_seg->ReleaseBucketsLock();
                    new_seg->lock.ReleaseLock();
                    seg->ReleaseBucketsLock();
                    seg->lock.ReleaseLock();
                }
            }
            else {
                // s3.4.3: double directory due to segment is only pointed by one entry
                lock.GetLock();
                auto d = __atomic_load_n(&dir, __ATOMIC_ACQUIRE);
                if (copy_dir->version != d->version) {
                    // s3.4.3.1: new directory has been allocated
                    lock.ReleaseLock();  goto RETRY;
                }  
                DirectoryDouble();
                lock.ReleaseLock();
            }

            // s3.4.3 retry insert
            goto RETRY;
        }

        return 0;
    }

    /* update: inplace for fixed-length key_value; out-of-place for varied-length kv */
    template <class KEY, class VALUE>
    bool HLSH<KEY, VALUE>::Update(Pair_t<KEY, VALUE>* p, size_t thread_id) {
        // s1: caculate hash key value for kv
        uint64_t key_hash = h(p->key(),p->klen());
    RETRY:
        // s2: find segment  
        auto seg = GetSegmentWithDirUpdate(key_hash);
        // s3: update kv
        auto r = seg->Update(p, key_hash, pm, thread_id, this);
        // s4: retry if failure to get lock to split
        if (rSegmentChanged == r) { goto RETRY; }

        return true;
    }

    /* get key value with option epoch */
    template <class KEY, class VALUE>
    bool HLSH<KEY, VALUE>::Get(Pair_t<KEY, VALUE> *p)
    {
        // GetNum++;
        // if (!(GetNum % PrefetchNum))
        // {
        //     for (size_t i = 0; i < PrefetchNum; i++)
        //     {
        //         if (pv[i].ok != nullptr)
        //         {
        // auto prep = reinterpret_cast<Pair_t<KEY, VALUE> *>(pv[i].ok);
        // for (auto kp : pv[i].kp)
        // {
        //     auto t = reinterpret_cast<Pair_t<KEY, VALUE> *>(kp);
        //     if (prep->str_key() == t->str_key())
        //     {
        //         prep->load(kp);
        //         auto tmp = __atomic_load_n(&t->fv.pad, __ATOMIC_ACQUIRE);
        //         FlagVersion fv(tmp);
        //         if (fv.get_flag() == FLAG_t::VALID)
        //         {
        //             break;
        //         }
        //     }
        // }
        //             pv[i].Clear();
        //         }
        //     }
        //     GetNum = 0;
        // }
        // s1: caculate hash value for key-value pair
        uint64_t key_hash = h(p->key(), p->klen());
        // s2: obtain segment
        auto seg = GetSegment(key_hash);
    RETRY:
        // s3: get value from segment
        int extra_rcode = 0;
        auto r = seg->Get(p, key_hash, extra_rcode);
        // s4: return value or retry
        if (rFailure == r) {
            if (seg != GetSegment(key_hash)) {
                goto RETRY;
            };
        }
        return r;
    }

    /* Delete: By default, the merge operation is disabled*/
    template <class KEY, class VALUE>
    bool HLSH<KEY, VALUE>::Delete(Pair_t<KEY, VALUE>* p, size_t thread_id) {
        // s1: caclulate the hash value for key-value pair
        uint64_t key_hash = h(p->key(), p->klen());
    RETRY:
        // s2: find segment
        auto seg = GetSegmentWithDirUpdate(key_hash);
        // s3: delete kv
        auto r = seg->Delete(p, key_hash, pm, this);
        // s4: retry if failure to get lock to split
        if (rSegmentChanged == r) {
            goto RETRY;
        }

        return true;
    }
}