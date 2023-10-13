#include "hlsh_directory.h"

namespace HLSH_hashing{
    template <class KEY, class VALUE>
    struct Directory;

    template <class KEY, class VALUE>
    struct PersistHash
    {
        bool Persisted; // true: persisted, false: do not persisted
        uint64_t capacity;   // length of directory
        uint64_t recovery_point[kThreadNum]; // recovery point for each thread
        uint64_t last_chunk[kListNum];
        std::atomic<uint64_t> offset; //offset for current file 
        uint64_t seg[0];

        inline bool IsPersisted()
        {
            return Persisted;
        }

        inline Directory<KEY, VALUE> *Recovery()
        {
            if (!Persisted)
                return nullptr;
            // s: allocate new dir
            Directory<KEY, VALUE> *dir;
            Directory<KEY, VALUE>::New(&dir, capacity, 0, nullptr, 64, nullptr, true);
            size_t global_depth = log2(capacity);
            dir->entries_update = true;
            // s: travese and recover
            auto base_addr = reinterpret_cast<uint64_t>(this);
            size_t seg_id = 0;
            auto load_seg = [&]()
            {
                auto sid = LOAD(&seg_id);
                while (sid < capacity)
                {
                    auto real_addr = reinterpret_cast<void *>(base_addr + seg[sid]);
                    auto pm_seg = reinterpret_cast<Segment<KEY, VALUE> *>(real_addr);
                    size_t chunk_size = pow(2, global_depth - pm_seg->local_depth);
                    if (CAS(&seg_id, &sid, sid + chunk_size))
                    {
                        Segment<KEY, VALUE>::New(&dir->_[sid], global_depth, sid);
                        memcpy(dir->_[sid], real_addr, sizeof(Segment<KEY, VALUE>));
                        // s: segment recovery
                        dir->_[sid]->Recovery();
                        for (size_t i = 1; i < chunk_size; i++)
                        {
                            dir->_[sid + i] = dir->_[sid];
                        }
                        sid = LOAD(&seg_id);
                    }
                }
            };
            std::vector<std::thread> vt;
            for (size_t i = 0; i < kThreadNum; i++)
            {
                vt.push_back(std::thread(load_seg));
            }
            for (auto &t : vt)
            {
                t.join();
            }
            return dir;
        }

        inline void wait()
        {
            auto b = LOAD(&Persisted);
            while (!b)
            {
                b = LOAD(&Persisted);
            };
        }

        inline void Init(Directory<KEY, VALUE> *d)
        {
            // s: initialization persist information
            STORE(&Persisted, false);
            capacity = d->capacity;
            // s: set offset
            offset.store(sizeof(PersistHash<KEY, VALUE>) +
                         sizeof(uint64_t) * capacity);
            // s: persist
            clwb_sfence(this, sizeof(PersistHash<KEY, VALUE>));
        }

        inline void Init(Directory<KEY, VALUE> *d,
                         uint64_t rpoint[], uint64_t lchunk[])
        {
            // s: initialization persist information
            STORE(&Persisted, false);
            capacity = d->capacity;
            // s: set recovery point
            for (size_t i = 0; i < kThreadNum; i++) {
                recovery_point[i] = rpoint[i];
            }
            // s: set last chunk
            for (size_t i = 0; i < kListNum; i++)
            {
                last_chunk[i] = lchunk[i];
            }
            offset.store(sizeof(PersistHash<KEY, VALUE>) +
                         sizeof(uint64_t) * capacity);
            // s: persist
            clwb_sfence(this, sizeof(PersistHash<KEY, VALUE>));
        }

        void PersistSegment(Segment<KEY, VALUE> *s)
        {
            // s: set index and persist segment
            auto addr = offset.fetch_add(sizeof(Segment<KEY, VALUE>));
            // s: calculate start position and range
            size_t global_depth = log2(capacity);
            size_t start_pos = s->pattern << (global_depth - s->local_depth);
            size_t chunk_size = pow(2, global_depth - s->local_depth);
            // s: set index and persist
            for (size_t k = 0; k < chunk_size; k++)
            {
                *(seg + start_pos + k) = addr;
            }
            // s: transfer to real address
            auto real_addr = reinterpret_cast<uint64_t>(this) + addr;
            // s: persist segment
            memcpy_persist512(reinterpret_cast<void *>(real_addr),
                              reinterpret_cast<void *>(s),
                              sizeof(Segment<KEY, VALUE>));
            // s: set nullptr which indicate that it don't need to persist
            s->ph = nullptr;
        }

        void BackPersistSegment(Directory<KEY, VALUE> *nd, Directory<KEY, VALUE> *od)
        {
            for (size_t i = 0; i < nd->capacity;)
            {
                // s: check whether the entry is updated
                auto s = nd->_[i];
                if (s) {
                    do {
                        i++;
                        if (i >= nd->capacity) break;
                        s = nd->_[i];
                    } while (s);
                    if (i >= nd->capacity) break;
                }
                // s: get lock
                s = od->_[i / 2];
                s->lock.GetLock();
                // s: persist segment
                if (s->ph){
                    PersistSegment(s);
                }
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
            STORE(&Persisted, true);
            clwb_sfence(&Persisted, sizeof(Persisted));
            STORE(&nd->entries_update, true);
        }

        static PersistHash<KEY, VALUE> *CreateNewPersist(
            Directory<KEY, VALUE> *d, uint64_t size,
            uint64_t r_point[], uint64_t l_chunk[])
        {
            // s: generate file name
            auto s = PM_PATH + std::string("DHLSH") + std::to_string(d->global_depth);
            auto index_file = s.c_str();
            // s: create and map new file
            auto fd = open(index_file, HLSH_FILE_OPEN_FLAGS, 0643);
            if (fd < 0) {
                printf("can't open file: %s\n", index_file);
            }
            if ((errno = posix_fallocate(fd, 0, size)) != 0) {
                perror("posix_fallocate");
                exit(1);
            }
            void *pmem_addr = mmap(nullptr, size, HLSH_MAP_PROT, HLSH_MAP_FLAGS, fd, 0); // mmap: align page if addr is 0
            if ((pmem_addr) == nullptr || (pmem_addr) == MAP_FAILED) {
                printf("Can't mmap\n");
            }
            close(fd);
            // s: init pm file
            auto index = reinterpret_cast<PersistHash<KEY, VALUE> *>(pmem_addr);
            index->Init(d, r_point, l_chunk);
            return index;
        }
    };
}