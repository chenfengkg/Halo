#include "hlsh_index_persist.h"

namespace HLSH_hashing {
std::atomic<uint64_t> insert_num{0};

template <class KEY, class VALUE>
class HLSH {
  Lock64 dir_lock;  
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
  inline bool Get(Pair_t<KEY, VALUE> *p);
  inline bool UpdateForReclaim(Pair_t<KEY, VALUE> *p,
                               PmOffset old_value, PmOffset new_value);
  inline void DirectoryDouble();
  inline void DirectoryUpdate(Directory<KEY, VALUE> *d,
                              Segment<KEY, VALUE> *new_seg);
  size_t GetDepth() { return dir->global_depth; }

  double GetTotalSlot()
  {
      return segment_num * Segment<KEY, VALUE>::GetSlotNum();
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

  std::pair<size_t, size_t> SpaceConsumption()
  {
      std::unordered_map<Segment<KEY, VALUE> *, bool> set;
      for (size_t i = 0; i < dir->capacity; i++)
      {
          set[dir->_[i]] = true;
      }
      size_t total_dram = (set.size() + remaining_chunk) * sizeof(Segment<KEY, VALUE>) +
                          sizeof(Directory<KEY, VALUE>) +
                          dir->capacity * sizeof(Segment<KEY, VALUE> *);
      size_t total_pmem = (pm->GetTotalUsedChunk() + kDimmNum) * kChunkSize;
      return {total_dram, total_pmem};
  }

  inline Segment<KEY,VALUE>* GetSegment(size_t key_hash) {
    // s1: get segment pointer from lastest dir
    auto d = LOAD(&dir);
    auto x = (key_hash >> (8 * sizeof(key_hash) - d->global_depth));
    // s2: get segment pointer from old dir if pointer is null
    auto seg = d->_[x];
    if (d->_[x] == nullptr)
    {
        seg = d->old_dir->_[x / 2];
    }
    return seg;
  }

  inline Segment<KEY,VALUE>* GetSegmentWithDirUpdate(
      size_t key_hash, Directory<KEY,VALUE>** copy_dir = nullptr) {
    // s1: get segment pointer from lastest dir
    auto d = LOAD(&dir);
    auto x = (key_hash >> (8 * sizeof(key_hash) - d->global_depth));
    Segment<KEY,VALUE>* seg = d->_[x];
    /* s2: get segment from old dir if pointer is null */
    if (!seg) {
      // s2.1: get valid segment from old dir
      auto old_dir = d->old_dir;
      seg = old_dir->_[x / 2];
      /* s2.2: update entries to point to this valid segment */
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
        Directory<KEY, VALUE>::New(&back_dir, initCap, 0, nullptr, 64, nullptr, true);
        dir = back_dir;
        Segment<KEY,VALUE>* ptr = nullptr, * next = nullptr;
        for (int i = initCap - 1; i >= 0; --i) {
            Segment<KEY, VALUE>::New(&ptr, dir->global_depth, i);
            // s2.1: init seg as split segment 
            dir->_[i] = ptr;
        }
        // s: init PM management
        pm = new PmManage<KEY, VALUE>(pool_file, pool_size, this);
        // s: crash or normal shutdown
        clean = false;

        printf("size, segment: %lu, bucket: %lu\n", sizeof(Segment<KEY, VALUE>),
               sizeof(Bucket<KEY, VALUE>));
    }

    template <class KEY, class VALUE>
    HLSH<KEY, VALUE>::HLSH(const char *pool_file, const size_t &pool_size)
    {
        bool is_recovery = false;
        std::cout << "Reinitialize up" << std::endl;
        // s2: recovery from shutdown file
        auto shutdown_file_name = PM_PATH + std::string("HLSH_SHUTDOWN");
        if (FileExists(shutdown_file_name.c_str()))
        {
            auto fd = open(shutdown_file_name.c_str(), HLSH_FILE_OPEN_FLAGS, 0643);
            void *fd_addr = mmap(nullptr, sizeof(PersistHash<KEY, VALUE>),
                                 HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
            close(fd);
            auto dindex = reinterpret_cast<PersistHash<KEY, VALUE> *>(fd_addr);
            if (dindex->IsPersisted())
            {
                fd = open(shutdown_file_name.c_str(), HLSH_FILE_OPEN_FLAGS, 0643);
                auto size = sizeof(PersistHash<KEY, VALUE>) +
                            sizeof(uint64_t) * dindex->capacity +
                            sizeof(Segment<KEY, VALUE>) * dindex->capacity;
                size = Round2StripeSize(size);
                fd_addr = mmap(nullptr, size,
                               HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
                // clean flush
                close(fd);
                dindex = reinterpret_cast<PersistHash<KEY, VALUE> *>(fd_addr);
                dir = dindex->Recovery();
                // s: open PM
                pm = new PmManage<KEY, VALUE>(pool_file, pool_size, this);
                pm->RecoveryWithoutData();
                // s: set recovery flag
                is_recovery = true;
            }
        }

        // s3: recover from checkpoint
        if (!is_recovery)
        {
            std::string ifile = "empty";
            size_t size = 0;
            size_t cap = 0;
            for (const auto &entry : std::filesystem::directory_iterator(PM_PATH))
            {
                std::string filename = entry.path().filename().string();
                if (filename.rfind("DHLSH", 0) == 0)
                {
                    auto fd = open(entry.path().c_str(), HLSH_FILE_OPEN_FLAGS, 0643);
                    void *fd_addr = mmap(nullptr, sizeof(PersistHash<KEY, VALUE>),
                                         HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0);
                    close(fd);
                    auto dindex = reinterpret_cast<PersistHash<KEY, VALUE> *>(fd_addr);
                    if (!dindex->IsPersisted())
                    {
                        break;
                    }
                    else
                    {
                        if (dindex->capacity > cap)
                        {
                            cap = dindex->capacity;
                            ifile = entry.path();
                            size = sizeof(PersistHash<KEY, VALUE>) + sizeof(uint64_t) * cap +
                                   sizeof(Segment<KEY, VALUE>) * cap;
                        }
                    }
                }
            }
            if (ifile != "empty")
            {
                printf("recovery file from: %s\n", ifile.c_str());
                // s: open file
                auto fd = open(ifile.c_str(), HLSH_FILE_OPEN_FLAGS, 0643);
                void *fd_addr = mmap(nullptr, size, HLSH_MAP_PROT,
                                     HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
                close(fd);
                // s: recovery checkpoint of dram index
                auto dindex = reinterpret_cast<PersistHash<KEY, VALUE> *>(fd_addr);
                dir = dindex->Recovery();
                // s: open PM
                pm = new PmManage<KEY, VALUE>(pool_file, pool_size, this);
                pm->RecoveryWithoutData();
                // s: recovery for chunklist
                auto rp = dindex->recovery_point;
                auto lc = dindex->last_chunk;
                std::vector<std::thread> vk;
                for (size_t i = 0; i < kThreadNum; i++)
                {
                    auto list_id = (rp[i] % kStripeSize) / kChunkSize;
                    if (lc[list_id] != rp[i])
                    {
                        auto chunk_list = pm->GetList(list_id);
                        vk.push_back(std::thread(&PmChunkList<KEY, VALUE>::RecoverChunk,
                                                 chunk_list, rp[i]));
                    }
                }
                for (auto &k : vk)
                {
                    k.join();
                }
                std::vector<std::thread> vt;
                for (size_t i = 0; i < kListNum; i++)
                {
                    auto chunk_list = pm->GetList(i);
                    STORE(&chunk_list->start_chunk, lc[i]);
                    for (size_t k = 0; k < (kMaxThreadNum / kListNum); k++)
                    {
                        vt.push_back(std::thread(&PmChunkList<KEY, VALUE>::RecoverIndex,
                                                 chunk_list));
                    }
                }
                for (auto &k : vt)
                {
                    k.join();
                }
                is_recovery = true;
            }
        }
        if (!is_recovery)
        {
            // s: open PM
            pm = new PmManage<KEY, VALUE>(pool_file, pool_size, this);
            // s: allocate new directory
            size_t depth = pm->GetDepth();
            Directory<KEY, VALUE>::New(&dir, pow(2, depth), 0,
                                       nullptr, 64, nullptr, true);
            size_t initCap = pow(2, dir->global_depth);
            Segment<KEY, VALUE> *ptr = nullptr;
            for (int i = initCap - 1; i >= 0; --i)
            {
                Segment<KEY, VALUE>::New(&ptr, dir->global_depth, i);
                dir->_[i] = ptr;
            }
            // s: recovery from start
            pm->RecoveryWithData();
        }
    }

    template <class KEY, class VALUE>
    HLSH<KEY, VALUE>::~HLSH(void) {
        // s: persist dram index
        pm->Shutdown(dir);

        printf("count: %lu, count1: %lu, count2: %lu, total: %lu\n",
               count.load(), count1.load(), count2.load(), count.load() + count1.load() + count2.load());
        printf("count3: %lu, count4: %lu, count5: %lu, total: %lu\n",
               count3.load(), count4.load(), count5.load(), count3.load() + count4.load() + count5.load());
        printf("extra_write_count: %lu\n", hlsh_write_count.load());
    }

    template <class KEY, class VALUE>
    void HLSH<KEY, VALUE>::DirectoryUpdate(Directory<KEY, VALUE> *d,
                                           Segment<KEY, VALUE> *new_seg)
    {
        // s1: calculate start pos and range
        size_t depth_diff = d->global_depth - new_seg->local_depth;
        auto start_pos = new_seg->pattern << depth_diff;
        size_t chunk_size = pow(2, depth_diff);
        // s2: update entries
        for (int k = 0; k < chunk_size; k++)
        {
            d->_[start_pos++] = new_seg;
        }
    }

    template <class KEY, class VALUE>
    void HLSH<KEY, VALUE>::DirectoryDouble()
    {
        // s1: persist new depth into PM
        auto global_depth = dir->global_depth;
        // pm->SetDepth(global_depth + 1);
        if (dir->NeedPersist())
        {
            // s: wait until old persist thread finish
            if (dir->ph)
                dir->ph->wait();
            // s: create new file for persisting index
            auto index_size = sizeof(PersistHash<KEY, VALUE>) +
                              sizeof(uint64_t) * dir->capacity +
                              sizeof(Segment<KEY, VALUE>) * dir->capacity;
            index_size = Round2StripeSize(index_size);
            uint64_t recovery_point[kThreadNum];
            uint64_t last_chunk[kListNum];
            pm->GetRecoveryPoint(recovery_point, last_chunk);
            auto persist_index = PersistHash<KEY, VALUE>::CreateNewPersist(
                dir, index_size, recovery_point, last_chunk);
            // s: allocate new directory
            auto capacity = pow(2, global_depth + 1);
            Directory<KEY, VALUE>::New(&back_dir, capacity, dir->version + 1, dir, 0, persist_index);
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
            STORE(&dir, back_dir);
            // s: start background thread to persist segment
            std::thread t(&PersistHash<KEY, VALUE>::BackPersistSegment, persist_index, dir, dir->old_dir);
            t.detach();
            // s: release lock
            dir_lock.ReleaseLock();
        }
        else
        {
            // s: wait last directory entries udpate
            while (!LOAD(&dir->entries_update))
            {
                asm("nop");
            }
            // s: allocate new directory
            auto capacity = pow(2, global_depth + 1);
            Directory<KEY, VALUE>::New(&back_dir, capacity, dir->version + 1,
                                       dir, dir->interval + 1, nullptr);
            // s: set new dir to dir
            STORE(&dir, back_dir);
            // s: set entries for new dir
            auto nd = dir;
            auto od = nd->old_dir;
            dir_lock.ReleaseLock();
            for (size_t i = 0; i < nd->capacity;)
            {
                // s: check whether the entry is updated
                auto s = nd->_[i];
                if (s)
                {
                    do
                    {
                        i++;
                        if (i >= nd->capacity)
                            break;
                        s = nd->_[i];
                    } while (s);
                    if (i >= nd->capacity)
                        break;
                }
                // s: get lock
                s = od->_[i / 2];
                s->lock.GetLock();
                // s: set entries for new directory
                size_t chunk_size = pow(2, nd->global_depth - s->local_depth);
                if (nd->_[i])
                {
                    s->lock.ReleaseLock();
                    i = i + chunk_size;
                    continue;
                }
                size_t start_pos = s->pattern << (nd->global_depth - s->local_depth);
                for (size_t j = 0; j < chunk_size; j++)
                {
                    nd->_[start_pos + j] = s;
                }
                // s: release lock and move to next entry
                s->lock.ReleaseLock();
                i = i + chunk_size;
            }
            STORE(&nd->entries_update, true);
        }
        // s: print directory
        // printf("DirectoryDouble towards->%u\n", global_depth + 1);
    }

    /* insert key_value to dash */
    template <class KEY, class VALUE>
    bool HLSH<KEY,VALUE>::Insert(Pair_t<KEY, VALUE>* p, size_t thread_id,
                        bool is_recovery) {
        // insert_num++;
        // if (!(insert_num % 40000))
        // {
        //     auto total_slot = GetTotalSlot();
        //     auto lf = (insert_num.load() * 1.0) / total_slot;
        //     printf("loadfactor: %lf\n", lf);
        // }
        // pm->Insert(p, thread_id);
        // return true;
        // s1: insert kv-pair to pm except recovery
#ifndef DRAM_INDEX
        if (!is_recovery)
        {
            tl_value.InitValue();
        }
#endif
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
                    if (seg->ph)
                    {
                        seg->ph->PersistSegment(seg);
                    }
                    // s: get the newest dir
                    auto d = LOAD(&dir);
                    if (d != copy_dir) {
                        copy_dir = d;
                    }
                    // s: get buckets lock
                    seg->GetBucketsLock();
                    // s: split segment
                    auto new_seg = seg->Split();
                    // s: update direcotry entries
                    DirectoryUpdate(copy_dir, new_seg);
                    // s: release lock for new seg
                    new_seg->ReleaseBucketsLock();
                    new_seg->lock.ReleaseLock();
                    // s: release lock for split seg
                    seg->ReleaseBucketsLock();
                    seg->lock.ReleaseLock();
                }
            }
            else {
                // s3.4.3: double directory due to segment is only pointed by one entry
                dir_lock.GetLock();
                auto d = LOAD(&dir);
                if (copy_dir->version != d->version)
                {
                    // s3.4.3.1: new directory has been allocated
                    dir_lock.ReleaseLock();
                    goto RETRY;
                }
                DirectoryDouble();
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
        if (rSegmentChanged == r)
        {
            goto RETRY;
        }

        return true;
    }

    template <class KEY, class VALUE>
    bool HLSH<KEY, VALUE>::UpdateForReclaim(Pair_t<KEY, VALUE> *p,
                                            PmOffset old_value,
                                            PmOffset new_value)
    {
        // s1: caculate hash key value for kv
        uint64_t key_hash = h(p->key(),p->klen());
    RETRY:
        // s2: find segment  
        auto seg = GetSegmentWithDirUpdate(key_hash);
        // s3: update kv
        auto r = seg->UpdateForReclaim(p, key_hash, old_value, new_value, pm, this);
        // s4: retry if failure to get lock to split
        if (rSegmentChanged == r)
        {
            goto RETRY;
        }
        // s: value may be deleted
        if (rFailure == r)
        {
            pm->Delete(new_value);
        }
        return true;
    }

    /* get key value with option epoch */
    template <class KEY, class VALUE>
    bool HLSH<KEY, VALUE>::Get(Pair_t<KEY, VALUE> *p)
    {
        // s1: caculate hash value for key-value pair
        uint64_t key_hash = h(p->key(), p->klen());
        // s2: obtain segment
        auto seg = GetSegment(key_hash);
    RETRY:
        // s3: get value from segment
        int extra_rcode = 0;
        auto r = seg->Get(p, key_hash, extra_rcode);
        // s4: return value or retry
        auto new_seg = GetSegment(key_hash);
        if (seg != new_seg)
        {
            seg = new_seg;
            goto RETRY;
        };
#ifdef READ_BUFFER
        RBUFFER[GetNum] = GETP_CHAR(p);
        GetNum++;
        if (GetNum == PrefetchNum)
        {
            for (size_t i = 0; i < PrefetchNum; i++)
            {
                auto cache_p = GETP_PAIR(RBUFFER[i]);
                auto pm_pair = GETP_PAIR(RADDRS[i]);
                pm_pair->cmp_key_and_load(cache_p);
            }
            GetNum = 0;
        }
#endif
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
        if (rSegmentChanged == r)
        {
            goto RETRY;
        }

        return true;
    }
}