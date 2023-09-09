#include "hlsh_lock.h"
#include <fcntl.h>
#include <asm/mman.h>
#include <sys/mman.h>

namespace HLSH_hashing {

#define CACHE_LINE_SIZE 64
    constexpr size_t kMaxIntValue = (uint64_t)-1;

    constexpr size_t kDimmNum = 4;
    constexpr size_t kDimmMaxAllowThread = 8;

    constexpr size_t kBlockSize = 4 * 1024;
    constexpr size_t kChunkSize = 4096;
    constexpr size_t kChunkNumPerDimm = kBlockSize / kChunkSize;
    constexpr size_t kStripeSize = kDimmNum * kBlockSize;

    constexpr size_t kLogNum = kStripeSize / kChunkSize;
    constexpr size_t kLogNumMask = kLogNum - 1;

    constexpr size_t kReclaimThreshold = kChunkSize / 2;
    constexpr size_t kReclaimChunkNum = 32;

    constexpr size_t kThreadNum = 32;

    static constexpr auto HLSH_MAP_PROT = PROT_WRITE | PROT_READ;
    static constexpr auto HLSH_MAP_FLAGS = MAP_SHARED_VALIDATE | MAP_SYNC;
    static constexpr auto HLSH_DRAM_MAP_FLAGS = MAP_ANONYMOUS | MAP_PRIVATE;
    static constexpr auto HLSH_FILE_OPEN_FLAGS = O_CREAT | O_RDWR | O_DIRECT;
    template <class KEY, class VALUE>
    class HLSH;


    struct PmOffset {
      uint64_t chunk_start_addr : 49;
      uint64_t offset : 15;

      PmOffset() : chunk_start_addr(0), offset(0) {}
      PmOffset(uint64_t csa, uint64_t of) : chunk_start_addr(csa), offset(of) {}
      bool operator==(const PmOffset& other) const {
        return chunk_start_addr == other.chunk_start_addr &&
               offset == other.offset;
      }
      void InitValue() {
        chunk_start_addr = 0;
        offset = 0;
      }
      void Set(uint64_t c, uint64_t o) {
        chunk_start_addr = c;
        offset = o;
      }
    }__attribute__((packed));

    PmOffset PO_NULL(0, 0); //NULL offset for PM

    thread_local PmOffset tl_value;

    union FlagOffset {
      struct {
        uint64_t flag : 2;     // free_chunk:0, service_chunk:1, victim_chunk:2,
                               // dest_chunk:3
        uint64_t offset : 54;  // offset in chunk
      } fo;
      uint64_t entire;
    };

    template <class KEY, class VALUE>
    struct PmChunk {
      FlagOffset foffset;  // offset
      uint64_t id;
      uint64_t free_size;
      uint64_t next;

      size_t Recovery(size_t addr, HLSH<KEY, VALUE>* index) {
        size_t recovery_count = 0;
        size_t csize = 0;
        // s1: caclulate chunk addr for each chunk
        auto chunk_start_addr = addr + id * kStripeSize + sizeof(PmChunk<KEY,VALUE>);
        // s2: traverse and insert valid key-value into index
        while (csize < foffset.fo.offset) {
          auto p = reinterpret_cast<Pair_t<KEY, VALUE>*>(chunk_start_addr);
          if (p->get_flag() == FLAG_t::VALID)
          {
              tl_value.Set(addr + id * kStripeSize, foffset.fo.offset);
              index->Insert(p, 0, true);
              recovery_count++;
          }
          csize += p->size();
          chunk_start_addr += p->size();
        }
        return recovery_count;
      }
    };

    /*pm management for per list*/
    template <class KEY, class VALUE>
    struct PmChunkList {
        uint64_t addr; //physic addr: metadata area
        uint64_t next; // head for service_chunk_lists
        uint64_t cur;  // tail for service_chunk_lists
        uint64_t free; // head for free_chunk_lists
        uint64_t tail; // tail for free_chunk_lists
        uint64_t prechunk_of_victim; // pre chunk for reclaim chunk
        uint64_t victim_chunk; // victim chunk
        uint64_t dst_chunk; // destination_chunk

        HLSH<KEY,VALUE>* index; //Index on DRAM
        uint64_t depth; // depth for hash table on DRAM

        /*first create*/
        void Create(uint64_t physic_addr, uint64_t stripe_num, HLSH<KEY,VALUE>* _index) {
            // s1: initial variable for chunk list head
            addr = physic_addr;
            next = kMaxIntValue;
            cur = 0;
            free = 1;
            tail = stripe_num - 1;
            victim_chunk = kMaxIntValue;
            dst_chunk = kMaxIntValue;
            prechunk_of_victim = 0;
            index = _index;
            // s2: initial meta for each chunk 
            for (size_t i = 1;i < stripe_num;i++) {
                // s2.1: get chunk address
                auto chunk_addr = addr + i * kStripeSize;
                auto chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_addr);
                // s2.2: set chunk metadata
                chunk->foffset.fo.flag = 0;
                chunk->foffset.fo.offset = sizeof(PmChunk<KEY,VALUE>);
                chunk->id = i;
                chunk->free_size = 0;
                if ((stripe_num - 1) != i) { chunk->next = i + 1; }
                else { chunk->next = kMaxIntValue; }
                // s2.3: persist chunk metadata
                clwb_sfence(chunk, sizeof(PmChunk<KEY,VALUE>));
            }
            // s3: persist chunk list head
            clwb_sfence(this, sizeof(PmChunkList<KEY,VALUE>));
        }

        void Print() {
            auto k = free;
            do {
                auto chunk_addr = addr + k * kStripeSize;
                auto chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_addr);
                printf("%lu ", chunk->id);
                k = chunk->next;
            } while (k != kMaxIntValue);
        }

        /* open*/
        void open(uint64_t physic_addr, HLSH<KEY,VALUE>* _index) {
            addr = physic_addr;
            index = _index;
        }

        /*recovery for current chunk list*/
        size_t Recovery(size_t tid) {
            set_affinity(tid);
            auto start_id = next;
            size_t recovery_count = 0;
            // s1: traverse chunk and recovery 
            while (start_id != kMaxIntValue) {
                auto chunk_start_addr = addr + start_id * kStripeSize;
                auto start_chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_start_addr);
                recovery_count += start_chunk->Recovery(addr, index);
                start_id = start_chunk->next;
            }
            return recovery_count;
        }

        inline uint64_t GetReclaimChunk() {
            // s1: get start chunk for traverse by prechuk_of_victim
            uint64_t prechunk_id = prechunk_of_victim, chunk_id = kMaxIntValue;
            if (prechunk_id == 0) {
                chunk_id = next;
            }
            else {
                auto chunk_addr = addr + prechunk_id * kStripeSize;
                auto chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_addr);
                chunk_id = chunk->next;
            }
            // s2: determine victim chunk by reclaim threshold
            while (1) {
                if (chunk_id != kMaxIntValue) {
                    auto chunk_addr = addr + chunk_id * kStripeSize;
                    auto chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_addr);
                    if (chunk.free_size < kReclaimThreshold)  break;
                    prechunk_id = chunk_id;
                    chunk_id = chunk->next;
                }
                else return -1;
            }
            prechunk_of_victim = prechunk_id;
            victim_chunk = chunk_id;
            clwb_sfence(this, kCacheLineSize);
            return chunk_id;
        }

        /*garbage:  merge fragmented data*/
        inline int Move2CurrentChunk() {
            // s1: obtain pointer to victim chunk and set flag
            auto chunk_addr = addr + victim_chunk * kStripeSize;
            auto chunk = reinterpret_cast<PmChunk<KEY, VALUE>*>(
                chunk_addr);  // victim chunk
            chunk->foffset.fo.flag = 2;
            clwb_sfence(&chunk->foffset, sizeof(uint64_t));

            // s2: obtain pointer to destination chunk and set flag
            auto dchunk = GetNewChunk(true);

            // s3: move valid data from victim chunk to destination chunk
            auto dst_start =
                reinterpret_cast<uint64_t>(dchunk) + dchunk->foffset.fo.offset;
            auto dst = reinterpret_cast<char*>(dst_start);
            auto victim_start = chunk_addr + chunk->foffset.fo.offset;
            auto src = reinterpret_cast<Pair_t<KEY, VALUE>*>(victim_start);
            int len = 0, write_size = 0;
            auto doffset = dchunk->foffset.fo.offset;
            do {
                auto kvsize = src->size();
                if ((kChunkSize - doffset) < kvsize) {
                    /* s3.1 destination chunk has full, flush destination chunk,
                     * and switch to next chunk*/
                    // s3.1.1 persist metadata for destination chunk
                    clwb_sfence(reinterpret_cast<void*>(dst_start), write_size);
                    dchunk->foffset.fo.offset += write_size;
                    dchunk->foffset.fo.flag = 1;
                    clwb_sfence(&dchunk->foffset, sizeof(FlagOffset));
                    write_size = 0;
                    // s3.1.2 get new destination chunk and update address for
                    // insert
                    dchunk = GetNewChunk(true);
                    dst_start = reinterpret_cast<uint64_t>(dchunk) +
                                dchunk->foffset.fo.offset;
                    dst = reinterpret_cast<char*>(dst_start);
                    doffset = dchunk->foffset.fo.offset;
                }
                if (src->flag) {
                    /* s3.2 move valid data to destination chunk*/
                    // s3.2.1 store into destination chunk
                    src->store(dst);
                    // s3.2.2 update index on DRAM
                    tl_value.Set(reinterpret_cast<uint64_t>(dchunk), doffset);
                    index->Insert(src, 0, true);
                    // s3.2.3 next position in destination chunk
                    dst += kvsize;
                    doffset += kvsize;
                    write_size += kvsize;
                }
                len += kvsize;
                src +=
                    reinterpret_cast<Pair_t<KEY, VALUE>*>(victim_start + len);
            } while (len < chunk->offset.offset);

            // s4: persist dst chunk data and metadata
            clwb_sfence(reinterpret_cast<void*>(dst_start), write_size);
            dchunk->foffset.fo.offset += write_size;
            clwb_sfence(&dchunk->foffset, sizeof(uint64_t));

            /* s6: move victim chunk to tail of the free list*/
            // s6.1: separate victim chunk from service chunk list
            auto pchunk_addr = addr + prechunk_of_victim * kStripeSize;
            auto pchunk = reinterpret_cast<PmChunk<KEY, VALUE>*>(pchunk_addr);
            pchunk->next = chunk->next;
            // s6.2: append to the tail of free list
            auto tchunk_addr = addr + tail * kStripeSize;
            auto tchunk = reinterpret_cast<PmChunk<KEY, VALUE>*>(
                tchunk_addr);  // destination chunk
            tchunk->next = victim_chunk;
            clwb_sfence(&tchunk->next, sizeof(uint64_t));
            // s6.3: move tail to the victim chunk and set victim chunk as the
            // last chunk
            tail = victim_chunk;
            clwb_sfence(&tail, sizeof(uint64_t));
            chunk->next = kMaxIntValue;
            // s6.4: update the flag to indicate free chunk
            chunk->foffset.fo.flag = 0;
            // s6.5: persist all metadata in one cacheline
            clwb_sfence(chunk, CACHE_LINE_SIZE);

            return 0;
        }

        /* reclaim process for current chunk list*/ 
        void Reclaim() {
            for (size_t i = 0;i < kReclaimChunkNum;i++) {
                // s1: get victim chunk for specific chunk list
                auto victim_chunk = GetReclaimChunk();
                if (victim_chunk == -1) return;
                // s2: move from victim chunk to destination chunk
                Move2CurrentChunk();
            }
        }

        /*Get new chunk: relcaim thread or worker thread*/
        PmChunk<KEY,VALUE>* GetNewChunk(bool is_for_reclaim = false) {
            // s1: return reclaim chunk for reclaim thread directly if dst chunk is avaiable
            if (is_for_reclaim) {
                auto vchunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(addr + dst_chunk * kStripeSize);
                if (vchunk->foffset.fo.flag == 3) return vchunk;
            }
            // s2: contend new empty chunk by change flag with CAS
            FlagOffset old_value, new_value;
            uint64_t chunk_id = (uint64_t)-1;
            PmChunk<KEY,VALUE>* chunk = nullptr;
            do {
                chunk_id = __atomic_load_n(&free, __ATOMIC_ACQUIRE);
                if (kMaxIntValue == chunk_id) return nullptr;
                auto chunk_addr = addr + free * kStripeSize;
                chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(chunk_addr);
                old_value.fo.flag = 0;
                old_value.fo.offset = sizeof(PmChunk<KEY,VALUE>);
                if (is_for_reclaim) new_value.fo.flag = 3;
                else new_value.fo.flag = 1;
                new_value.fo.offset = sizeof(PmChunk<KEY,VALUE>);
            } while (!CAS(&chunk->foffset.entire, &old_value.entire, new_value.entire));
            clwb_sfence(&chunk->foffset, sizeof(uint64_t));
            // s3: link new chunk to sevices chunk list
            if (is_for_reclaim) {
                // s3.1: link new chunk to head for reclaim thread
                // s3.1.1: new destination chunk
                dst_chunk = chunk_id;
                // s3.1.2: move free to next chunk
                free = chunk->next;
                clwb_sfence(this, CACHE_LINE_SIZE);
                // s3.1.3: add new chunk to service chunk list 
                chunk->next = next;
                clwb_sfence(&chunk->next, sizeof(uint64_t));
                next = chunk_id;
                // s3.1.4: update prechunk_of_victim if it points to the head
                if (prechunk_of_victim == 0)
                    prechunk_of_victim = chunk_id;
                clwb_sfence(this, CACHE_LINE_SIZE);
            }
            else {
                // s3.2: link new chunk to cur for worker thread
                // s3.2.1: link new chunk to service chunk list
                if (0 == cur) {
                    // s3.2.1.1: cur chunk is the head, make next pointer pointing to new chunk
                    next = chunk_id;
                    clwb_sfence(&next, sizeof(uint64_t));
                }
                else {
                    // s3.2.1.2: cur chunk is normal chunk
                    auto cur_chunk_addr = addr + cur * kStripeSize;
                    auto cur_chunk = reinterpret_cast<PmChunk<KEY,VALUE>*>(cur_chunk_addr);
                    // s3.2.1.3: make next pointer of cur chunk pointing to new chunk
                    cur_chunk->next = chunk_id;
                    clwb_sfence(&chunk->next, sizeof(uint64_t));
                }
                // s3.2.3: make cur point to new chunk
                cur = chunk_id;
                // s3.2.4: move free to next chunk
                free = chunk->next;
                clwb_sfence(this, CACHE_LINE_SIZE);
                // s3.2.5: separate new chunk from free list
                chunk->next = kMaxIntValue;
                clwb_sfence(&chunk->next, sizeof(uint64_t));
            }
            return chunk;
        }
    };
}
