#include "hlsh_pm_allocator.h"

namespace HLSH_hashing {
    std::string PM_PATH = "/data/pmem0/";
    template <class KEY, class VALUE>
    struct PersistHash;
    template <class KEY, class VALUE>
    struct Directory;
    template <class KEY, class VALUE>
    struct Segment;
    /*meta on DRAM: thread-local*/
    template <class KEY, class VALUE>
    struct alignas(64) ThreadMeta {
        uint64_t start_addr;
        uint64_t offset;
        uint64_t dimm_no;  // belong to which dimm
        uint64_t chunk_addr; // chunk addr relative to file 

        ThreadMeta() {}

        PmOffset Insert(Pair_t<KEY, VALUE>* p) {
          // s1: judge remain space is enough to accomodate the key-value pair
          auto len = p->size();
          if (kChunkSize < (len + offset + sizeof(uint8_t)))
          {
              // s: flush chunk metadata
              auto chunk = reinterpret_cast<PmChunk<KEY, VALUE> *>(start_addr);
              chunk->foffset.fo.flag = CHUNK_FLAG::FULL_CHUNK;
              chunk->foffset.fo.offset = offset;
              clwb_sfence(&chunk->foffset, sizeof(FlagOffset));
              return PO_NULL;
          }
          // s2: obtain the insertion address
          p->store_persist(reinterpret_cast<char*>(start_addr + offset));
          // s3: construct PM offset
          PmOffset of(chunk_addr, offset);
          // s4: increase offset (not flush)
          offset += len;
          return of;
        }
    };

    /* meta on DRAM: used for limiting thread number*/
    struct alignas(64) DimmMeta {
        uint64_t allocated_memory_block;
        std::atomic<uint64_t> concurrent_thread_num;
        std::mutex thread_limit_lock;

        DimmMeta() {
            allocated_memory_block = 0;
            concurrent_thread_num = 0;
        }

        inline void EnterDimm() {
            // std::lock_guard<std::mutex> lock(thread_limit_lock);
            while (concurrent_thread_num.load(std::memory_order_acquire) >= kDimmMaxAllowThread);
            concurrent_thread_num++;
        }

        inline void ExitDimm() {
            concurrent_thread_num--;
        }
    };

    template <class KEY, class VALUE>
    struct alignas(64) PmManage {
        ThreadMeta<KEY, VALUE> tm[kThreadNum];
        DimmMeta dm[kDimmNum];
        uint64_t last_chunk[kListNum]; //chunk addr
        std::mutex mutex_lock;

        inline void GetRecoveryPoint(uint64_t recovery_point[],
                                     uint64_t _last_chunk[])
        {
            std::lock_guard<std::mutex> lock(mutex_lock);
            // s: get chunk addr
            for (size_t i = 0; i < kThreadNum; i++)
            {
                recovery_point[i] = tm[i].chunk_addr;
            }
            for (size_t i = 0; i < kListNum; i++)
            {
                _last_chunk[i] = last_chunk[i];
            }
        }

        size_t GetDepth() {
            // s1: get pointer of the first chunk list head
            auto chunk = reinterpret_cast<PmChunkList<KEY, VALUE> *>(pmem_start_addr);
            // s2: return depth
            return chunk->depth;
        }

        void SetDepth(size_t _depth) {
            // s1: get pointer of the first chunk list head
            auto chunk = reinterpret_cast<PmChunkList<KEY, VALUE> *>(pmem_start_addr);
            // s2: update and persist depth
            chunk->depth = _depth;
            clwb_sfence(&chunk->depth, sizeof(uint64_t));
        }

        void ReclaimTest(){
            for (size_t i = 0; i < kListNum; i++)
            {
                auto chunk_list = GetList(i);
                bool b = false;
                if (CAS(&chunk_list->is_reclaim, &b, true))
                {
                    chunk_list->Reclaim();
                }
            }
        }

        inline void RecoveryWithData()
        {
            RecoveryListMetadata();
            RecoveryListData();
            RecoveryInsertMetadata();
        }

        inline void RecoveryWithoutData()
        {
            RecoveryListMetadata();
            RecoveryInsertMetadata();
        }

        inline void RecoveryListMetadata()
        {
            // s1: recover chunk list
            for (size_t i = 0; i < kListNum; i++)
            {
                auto chunk_list = GetList(i);
                // s: recover crash during allocate
                chunk_list->RecoverAllocate();
                // s: recover crash during allocate
                chunk_list->RecoverReclaim();
            }
        }

        inline void RecoveryInsertMetadata()
        {
            // s: recovery thread metadata
            for (size_t i = 0; i < kThreadNum; i++)
            {
                GetNewChunk(0, i, false);
            }
        }

        inline void RecoveryListData()
        {
            // s: recover index
            std::vector<std::thread> vt;
            for (size_t i = 0; i < kListNum; i++)
            {
                auto chunk_list = GetList(i);
                for (size_t k = 0; k < (kMaxThreadNum / kListNum); k++)
                {
                    vt.push_back(std::thread(&PmChunkList<KEY, VALUE>::RecoverIndex,
                                             chunk_list));
                }
            }
            // s2: wait task finish
            for (auto &t : vt)
            {
                t.join();
            }
        }

        static uint64_t Create(const char *pool_file, const size_t &pool_size,
                               HLSH<KEY, VALUE> *index)
        {
            // s: create file and map pmem to memory
            auto fd = open(pool_file, HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0)
            {
                printf("can't open file: %s\n", pool_file);
            }
            if ((errno = posix_fallocate(fd, 0, pool_size)) != 0)
            {
                perror("posix_fallocate");
                exit(1);
            }
            void *pmem_addr = mmap(nullptr, pool_size, HLSH_MAP_PROT,
                                   HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED)
            {
                printf("Can't mmap\n");
            }
            close(fd);
            // s: set pmem_start_addr
            pmem_start_addr = reinterpret_cast<uint64_t>(pmem_addr);
            // s: using multiple thread to intialize each chunk list information
            std::vector<std::thread> vt;
            auto stripe_num = pool_size / kStripeSize;
            for (size_t i = 0; i < kListNum; i++)
            {
                auto list_start_addr = pmem_start_addr + kChunkSize * i;
                auto list = reinterpret_cast<PmChunkList<KEY, VALUE> *>(
                    pmem_start_addr + kChunkSize * i);
                vt.push_back(std::thread(&PmChunkList<KEY, VALUE>::Create, list, kChunkSize * i,
                                         i, stripe_num, index));
            }
            for (auto &t : vt)
            {
                t.join();
            }
            printf("PM Create finished, addr: %lx\n", pmem_start_addr);
            return pmem_start_addr;
        }

        static uint64_t Open(const char *pool_file, const size_t &pool_size,
                               HLSH<KEY, VALUE> *index)
        {
            // s1: open file and map pmem to memory
            auto fd = open(pool_file, HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0)
            {
                printf("can't open file: %s\n", pool_file);
            }
            void *pmem_addr = mmap(nullptr, pool_size, HLSH_MAP_PROT,
                                   HLSH_MAP_FLAGS, fd, 0);
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED)
            {
                printf("Can't mmap\n");
            }
            close(fd);
            // s: set pmem addr as global variable
            pmem_start_addr = reinterpret_cast<uint64_t>(pmem_addr);
            // s3: intial each chunk list information
            for (size_t i = 0; i < kListNum; i++)
            {
                auto list = reinterpret_cast<PmChunkList<KEY, VALUE> *>(
                    pmem_start_addr + kChunkSize * i);
                list->Open(kChunkSize * i, index);
                clwb_sfence(list, sizeof(PmChunkList<KEY, VALUE>)); // persist
            }

            printf("PM Open finished, addr: %lx\n", pmem_start_addr);
            return pmem_start_addr;
        }

        void TerminateReclaim()
        {
            // s: terminate all reclaim
            for (size_t i = 0; i < kListNum; i++)
            {
                auto chunk_list = GetList(i);
                STORE(&chunk_list->terminate_reclaim, true);
            }
            // s: check if all chunk list has stop reclaim
            for (int i = 0; i < kListNum; i++)
            {
                auto chunk_list = GetList(i);
                if (LOAD(&chunk_list->is_reclaim))
                {
                    i = -1;
                }
            }
        }

        void Shutdown(Directory<KEY, VALUE> *dir)
        {
            printf("Start persist dram index\n");
            // s: terminate reclaim thread
            TerminateReclaim();
            // s: wait entries update finish
            auto entries_update = LOAD(&dir->entries_update);
            while (!entries_update)
            {
                entries_update = LOAD(&dir->entries_update);
            }
            // s: create new file to store dram index
            auto file_name = PM_PATH + std::string("HLSH_SHUTDOWN");
            auto fd = open(file_name.c_str(), HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0) {
                printf("can't open file: %s\n", file_name.c_str());
            }
            auto size = sizeof(PersistHash<KEY, VALUE>) +
                              sizeof(uint64_t) * dir->capacity +
                              sizeof(Segment<KEY, VALUE>) * dir->capacity;
            size = Round2StripeSize(size);
            if ((errno = posix_fallocate(fd, 0, size)) != 0) {
                perror("posix_fallocate");
                exit(1);
            }
            void *pmem_addr = mmap(nullptr, size, HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED) {
                printf("Can't mmap\n");
            }
            close(fd);
            auto index = reinterpret_cast<PersistHash<KEY, VALUE> *>(pmem_addr);
            index->Init(dir);
            // s: flush all segment with multiple thread
            uint64_t flush_id = 0;
            auto flush_thread = [&]()
            {
                auto fi = LOAD(&flush_id);
                while (fi < dir->capacity)
                {
                    auto s = dir->_[fi];
                    size_t chunk_size = pow(2, dir->global_depth - s->local_depth);
                    if (CAS(&flush_id, &fi, fi + chunk_size))
                    {
                        index->PersistSegment(s);
                        fi = LOAD(&flush_id);
                    }
                }
            };
            std::vector<std::thread> vt;
            for (size_t i = 0; i < kMaxThreadNum; i++)
            {
                vt.push_back(std::thread(flush_thread));
            }
            for (auto &t : vt)
                t.join();
            index->Persisted = true;
            clwb_sfence(&index->Persisted, sizeof(bool));
            printf("finish persisting dram index\n");
        }
        inline PmChunk<KEY,VALUE>* GetChunk(size_t chunk_addr){
            return reinterpret_cast<PmChunk<KEY, VALUE> *>(
                pmem_start_addr + chunk_addr);
        }

        inline PmChunkList<KEY,VALUE>* GetList(size_t list_id){
            return reinterpret_cast<PmChunkList<KEY, VALUE> *>(
                pmem_start_addr + kChunkSize * list_id);
        }

        PmChunk<KEY, VALUE> *GetNewChunkFromDimm(size_t dimm_id,
                                                 size_t &log_id)
        {
            PmChunk<KEY, VALUE> *new_chunk = nullptr;
            // s1: obtain dimm address
            auto dimm_addr = pmem_start_addr + dimm_id * kChunkNumPerDimm * kChunkSize;
            auto start_list_id = dimm_id * kChunkNumPerDimm;
            // s2: get a new chunk by traversing all chunk list in the dimm
            for (size_t i = 0; i < kChunkNumPerDimm; i++)
            {
                auto list = reinterpret_cast<PmChunkList<KEY, VALUE> *>(dimm_addr + i * kChunkSize);
                // s: lock for each chunk list
                new_chunk = list->GetNewChunk();
                if (new_chunk)
                {
                    log_id = start_list_id + i;
                    return new_chunk;
                }
            }
            return new_chunk;
        }

        /* get memory from dimm that has minimal number of allocated memory;*/
        inline PmChunk<KEY, VALUE> *GetNewChunk(int dimm_id, size_t tid,
                                                bool own_old_memory = true)
        {
            // s1: decrease memory counter
            int min_dimm = dimm_id;
            {
                std::lock_guard<std::mutex> lock(mutex_lock);
                if (own_old_memory)
                    dm[dimm_id].allocated_memory_block--;
                // s2: find the dimm with minimal number of allocated memory
                auto min_allocated_memory = kMaxIntValue;
                for (size_t k = 0; k < kDimmNum; k++)
                {
                    if (dm[k].allocated_memory_block < min_allocated_memory)
                    {
                        min_allocated_memory = dm[k].allocated_memory_block;
                        min_dimm = k;
                    }
                }
                dm[min_dimm].allocated_memory_block++;
            }
            // s3: get memory from dimm 
            size_t log_id = (size_t)-1;
            PmChunk<KEY, VALUE> *chunk = nullptr;
            auto k = min_dimm;
            do
            {
                for (size_t i = 0; i < kDimmNum; i++)
                {
                    // s: traverse to get a empyt chunk
                    chunk = GetNewChunkFromDimm((i + k) % kDimmNum, log_id);
                    if (chunk)
                        break;
                }
            } while (!chunk);
            {
                std::lock_guard<std::mutex> lock(mutex_lock);
                if (k != min_dimm)
                {
                    dm[min_dimm].allocated_memory_block--;
                    dm[k].allocated_memory_block++;
                }
            }
            // s4: set new information for ThreadMeta 
            tm[tid].start_addr = reinterpret_cast<uint64_t>(chunk);
            tm[tid].chunk_addr = chunk->chunk_addr;
            tm[tid].offset = chunk->foffset.fo.offset;
            tm[tid].dimm_no = min_dimm;
            // s: set last chunk
            last_chunk[log_id] = tm[tid].chunk_addr;
            return chunk;
        }

        PmManage(const char *pool_file, size_t pool_size,
                 HLSH<KEY, VALUE> *index)
        {
            // s1: init
            for (size_t i = 0; i < kListNum; i++)
                last_chunk[i] = kMaxIntValue;
            if (!FileExists(pool_file))
            {
                // s2: create pool
                Create(pool_file, pool_size, index);
                // s3: initial threadlocal metadata
                for (size_t i = 0; i < kThreadNum; i++)
                {
                    GetNewChunk(0, i, false);
                }
            }
            else
            {
                // s2: open file
                Open(pool_file, pool_size, index);
            }
        }

        inline PmOffset Insert(Pair_t<KEY, VALUE>* p, size_t tid) {
            //s1: acquire permission to enter dimm
            auto dimm_no = tm[tid].dimm_no;
            //s2: insert key-value pairs into pm
            auto pf = tm[tid].Insert(p);
            if (PO_NULL == pf) {
                // s2.1: get new chunk due to old chunk is full
                auto chunk = GetNewChunk(dimm_no, tid);
                if (!chunk) {
                    std::cerr << "no enough memory!" << std::endl;
                    exit(1);
                }
                pf = tm[tid].Insert(p);
            }
            return pf;
        }

        inline PmOffset Update(Pair_t<KEY, VALUE> *p, size_t tid, PmOffset po)
        {
            // s1: insert new key-value with higher version into the pm
            auto op = reinterpret_cast<Pair_t<KEY, VALUE> *>(po.GetValue());
            p->set_version(op->get_version() + 1);
            auto pf = Insert(p, tid);
            // s2: remove old key-value
            op->set_flag(FLAG_t::INVALID);
            clwb_sfence(reinterpret_cast<char *>(op), sizeof(FVERSION));
            // s3: free space for old key-value
            auto chunk = reinterpret_cast<PmChunk<KEY, VALUE> *>(po.GetChunk());
            uint64_t len = op->size();
            chunk->free_size += len;
            clwb_sfence(&chunk->free_size, sizeof(uint64_t));
            return pf;
        }

        /*Delete kv pair*/
        inline void Delete(PmOffset po) {
            // s1: invalid and persist flag
            auto p = reinterpret_cast<Pair_t<KEY, VALUE>*>(po.GetValue());
            p->set_flag(FLAG_t::INVALID);
            clwb_sfence(reinterpret_cast<char *>(p), sizeof(FVERSION));
            // s2: increase and persist free size
            auto chunk = reinterpret_cast<PmChunk<KEY, VALUE> *>(po.GetChunk());
            chunk->free_size += p->size();
            clwb_sfence(&chunk->free_size, sizeof(uint64_t));
        }
    };
}