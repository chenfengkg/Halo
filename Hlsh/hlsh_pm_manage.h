#include "hlsh_pm_allocator.h"

namespace HLSH_hashing {
    constexpr size_t kDelaySize = 256;
    constexpr size_t kInvalidAddr = 1UL << 48;


    /*meta on DRAM: thread-local*/
    template <class KEY, class VALUE>
    struct alignas(64) ThreadMeta {
        uint64_t start_addr;
        uint64_t offset;
        uint64_t dimm_no;  // belong to which dimm

        ThreadMeta() {}

        PmOffset Insert(Pair_t<KEY, VALUE>* p) {
          // s1: judge remain space is enough to accomodate the key-value pair
          auto len = p->size();
          if ((kChunkSize - offset) < len){
              clwb_sfence(reinterpret_cast<char *>(start_addr), sizeof(PmChunk<KEY, VALUE>));
              return PmOffset(kInvalidAddr, 0);
          }
          // s2: obtain the insertion address
          p->store_persist(reinterpret_cast<char*>(start_addr + offset));
          // s3: construct PM offset
          PmOffset of(start_addr, offset);
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
        VersionLock new_chunk_lock;

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
        uint64_t pool_addr;
        std::mutex mutex_lock;

        size_t GetDepth() {
            // s1: get pointer of the first chunk list head
            auto chunk = reinterpret_cast<PmChunkList<KEY, VALUE>*>(pool_addr);
            // s2: return depth
            return chunk->depth;
        }

        void SetDepth(size_t _depth) {
            // s1: get pointer of the first chunk list head
            auto chunk = reinterpret_cast<PmChunkList<KEY, VALUE>*>(pool_addr);
            // s2: update and persist depth
            chunk->depth = _depth;
            clwb_sfence(&chunk->depth, sizeof(uint64_t));
        }

        size_t Recovery() {
            std::vector<std::thread> td;
            // s1: travese each chunk list and assign one dedicated thread for each chunk list
            for (size_t i = 0; i < kThreadNum; i++)
            {
                // s1.1: get chunk list head
                auto chunk_list = reinterpret_cast<PmChunkList<KEY,VALUE>*>(pool_addr + i * kChunkSize);
                // s1.2: assign task to each thread
                td.push_back(std::thread(&PmChunkList<KEY,VALUE>::Recovery, chunk_list, i));
            }
            // s2: wait task finish 
            for (auto& t : td) {
                t.join();
            }
            return 0;
        }

        static uint64_t Create(const char* pool_file, const size_t& pool_size, HLSH<KEY,VALUE>* index) {
            // s1: create file and map pmem to memory
            auto fd = open(pool_file, HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0) { printf("can't open file: %s\n", pool_file); }
            if ((errno = posix_fallocate(fd, 0, pool_size)) != 0) { perror("posix_fallocate"); exit(1); }
            void* pmem_addr = mmap(nullptr, pool_size, HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0);//mmap: align page if addr is 0
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED) { printf("Can't mmap\n"); }
            close(fd);

            // s3: using multiple thread to intialize each chunk list information
            std::vector<std::thread> vt;
            auto stripe_num = pool_size / kStripeSize;
            auto start_addr = reinterpret_cast<uint64_t>(pmem_addr);
            for (size_t i = 0;i < kLogNum;i++) {
                auto list_start_addr = start_addr + kChunkSize * i;
                auto list = reinterpret_cast<PmChunkList<KEY,VALUE>*>(list_start_addr);
                vt.push_back(std::thread(&PmChunkList<KEY,VALUE>::Create, list, list_start_addr, stripe_num, index));
            }
            for (auto& t : vt) {
                t.join();
            }
            printf("PM Create and initialization finished, addr: %lx\n", start_addr);
            return start_addr;
        }

        static uint64_t Open(const char* pool_file, const size_t& pool_size, HLSH<KEY,VALUE>* index) {
            // s1: open file and map pmem to memory
            auto fd = open(pool_file, HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0) { printf("can't open file: %s\n", pool_file); }
            void* pmem_addr = mmap(nullptr, pool_size, HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0);
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED) { printf("Can't mmap\n"); }
            close(fd);
            
            // s3: intial each chunk list information
            auto start_addr = reinterpret_cast<uint64_t>(pmem_addr);
            for (size_t i = 0;i < kLogNum;i++) {
                auto list_start_addr = start_addr + kChunkSize * i;
                auto list = reinterpret_cast<PmChunkList<KEY,VALUE>*>(list_start_addr);
                list->Open(list_start_addr, index);
                clwb_sfence(list, sizeof(PmChunkList<KEY,VALUE>));//persist
            }

            printf("PM Open finished, addr: %lx", start_addr);
            return start_addr;
        }

        PmChunk<KEY, VALUE>* GetNewChunkFromDimm(size_t dimm_id) {
            PmChunk<KEY,VALUE>* new_chunk = nullptr;
            // s1: obtain dimm address
            auto dimm_addr = pool_addr + dimm_id * kChunkNumPerDimm * kChunkSize;
            // s2: get a new chunk by traversing all chunk list in the dimm 
            for (size_t i = 0;i < kChunkNumPerDimm;i++) {
                auto list = reinterpret_cast<PmChunkList<KEY,VALUE>*>(dimm_addr + i * kChunkSize);
                new_chunk = list->GetNewChunk();
                if (new_chunk != nullptr) return new_chunk;
            }
            return new_chunk;
        }

        /* get memory from dimm that has minimal number of allocated memory;*/
        inline PmChunk<KEY,VALUE>* GetNewChunk(int dimm_id, size_t tid, bool own_old_memory = true) {
            // s1: decrease memory counter
            std::lock_guard<std::mutex> lock(mutex_lock);
            if (own_old_memory) dm[dimm_id].allocated_memory_block--;
            // s2: find the dimm with minimal number of allocated memory
            auto min_allocated_memory = kMaxIntValue;
            int min_dimm = dimm_id;
            for (size_t k = 0;k < kDimmNum;k++) {
                int i = (dimm_id + k) % kDimmNum;
                if (dm[i].allocated_memory_block < min_allocated_memory) {
                    min_allocated_memory = dm[i].allocated_memory_block;
                    min_dimm = i;
                };
            }
            // s3: get memory from dimm and increase memory counter
            auto chunk = GetNewChunkFromDimm(min_dimm);
            dm[min_dimm].allocated_memory_block++;
            // s4: set new information for ThreadMeta 
            tm[tid].start_addr = reinterpret_cast<uint64_t>(chunk);
            tm[tid].offset = chunk->foffset.fo.offset;
            tm[tid].dimm_no = min_dimm;

            return chunk;
        }

        PmManage(const char* pool_file, size_t pool_size,
                 HLSH<KEY, VALUE>* index) {
            // s1: open file
            pool_addr = Create(pool_file, pool_size, index);
            // s2: initial value
            for (size_t i = 0; i < kThreadNum; i++) {
                GetNewChunk(0, i, false);
            }
        }

        inline PmOffset Insert(Pair_t<KEY, VALUE>* p, size_t tid) {
            //s1: acquire permission to enter dimm
            auto dimm_no = tm[tid].dimm_no;
            // dm[dimm_no].EnterDimm();
            //s2: insert key-value pairs into pm
            auto pf = tm[tid].Insert(p);
            if (kInvalidAddr == pf.chunk_start_addr) {
                // s2.1: get new chunk due to old chunk is full
                auto chunk = GetNewChunk(dimm_no, tid);
                if (!chunk) {
                    std::cerr << "no enough memory!" << std::endl;
                    exit(1);
                }
                pf = tm[tid].Insert(p);
            }
            //s3: release permission to exit dimm
            // dm[dimm_no].ExitDimm();
            return pf;
        }

        inline PmOffset Update(Pair_t<KEY, VALUE>* p, size_t tid, PmOffset po) {
            // s1: insert new key-value with higher version into the pm
            auto op = reinterpret_cast<Pair_t<KEY, VALUE>*>(
                po.chunk_start_addr + po.offset);
            p->set_version(op->get_version() + 1);
            auto pf = Insert(p, tid);
            // s2: remove old key-value
            op->set_flag(FLAG_t::INVALID);
            clwb_sfence(reinterpret_cast<char*>(op), sizeof(FLAG_VERSION));
            // s3: free space for old key-value
            auto chunk =
                reinterpret_cast<PmChunk<KEY, VALUE>*>(po.chunk_start_addr);
            uint64_t len = op->size();
            chunk->free_size += len;
            clwb_sfence(&chunk->free_size, sizeof(uint64_t));

            return pf;
        }

        /*Delete kv pair*/
        inline void Delete(PmOffset po) {
            // s1: invalid and persist flag
            auto p = reinterpret_cast<Pair_t<KEY, VALUE>*>(po.chunk_start_addr +
                                                           po.offset);
            p->set_flag(FLAG_t::INVALID);
            clwb_sfence(reinterpret_cast<char*>(p), sizeof(FLAG_VERSION));
            // s2: increase and persist free size
            auto chunk =
                reinterpret_cast<PmChunk<KEY, VALUE>*>(po.chunk_start_addr);
            uint64_t len = p->size();
            chunk->free_size += len;
            clwb_sfence(&chunk->free_size, sizeof(uint64_t));
        }
    };
}