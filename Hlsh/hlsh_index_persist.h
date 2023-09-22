#include "hlsh_directory.h"

namespace HLSH_hashing{
    template <class KEY, class VALUE>
    struct PersistHash
    {
        bool Persisted; // true: persisted, false: do not persisted
        uint64_t capacity;   // length of directory
        uint64_t recovery_point[kThreadNum]; // recovery point for each thread
        std::atomic<uint64_t> offset; //offset for current file 
        uint64_t seg[0];

        inline void wait()
        {
            auto b = __atomic_load_n(&Persisted, __ATOMIC_ACQUIRE);
            while (!b)
            {
                b = __atomic_load_n(&Persisted, __ATOMIC_ACQUIRE);
            };
        }

        void Init(Directory<KEY, VALUE> *d, uint64_t rpoint[])
        {
            // s: initialization persist information
            __atomic_store_n(&Persisted, false, __ATOMIC_RELEASE);
            capacity = d->capacity;
            // s: set recovery point
            for (size_t i = 0; i < kThreadNum; i++) {
                recovery_point[i] = rpoint[i];
            }
            offset.store(sizeof(PersistHash<KEY, VALUE>) + sizeof(uint64_t) * capacity);
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
            memcpy_persist(reinterpret_cast<void *>(real_addr), reinterpret_cast<void *>(s), sizeof(Segment<KEY, VALUE>));
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
                if (s->ph)
                    PersistSegment(s);
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
            __atomic_store_n(&Persisted, true, __ATOMIC_RELEASE);
        }

        static PersistHash<KEY, VALUE> *CreateNewPersist(Directory<KEY, VALUE> *d, uint64_t size, uint64_t r_point[])
        {
            // s: generate file name
            auto s = std::string("/data/pmem0/") + std::string("HLSH") + std::to_string(d->global_depth);
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
            index->Init(d, r_point);
            return index;
        }
    };
}