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

    constexpr size_t kListNum = kStripeSize / kChunkSize;
    constexpr size_t kListNumMask = kListNum - 1;

    constexpr size_t kMaxReclaimThreshold = kChunkSize / 2;
    constexpr size_t kMinReclaimThreshold = kChunkSize / 32;

    constexpr size_t kMaxThreadNum = 36;
    constexpr size_t kThreadNum = 36;

    static constexpr auto HLSH_MAP_PROT = PROT_WRITE | PROT_READ;
    static constexpr auto HLSH_MAP_FLAGS = MAP_SHARED_VALIDATE | MAP_SYNC;
    static constexpr auto HLSH_DRAM_MAP_FLAGS = MAP_ANONYMOUS | MAP_PRIVATE;
    static constexpr auto HLSH_FILE_OPEN_FLAGS = O_CREAT | O_RDWR | O_DIRECT;
    template <class KEY, class VALUE>
    class HLSH;
    
    uint64_t pmem_start_addr = 0;
    std::mutex list_lock[kListNum]; //chunk for each chunk list

#define GETP_CHUNK(addr) (reinterpret_cast<PmChunk<KEY, VALUE> *>(pmem_start_addr + (addr)))
#define GETP_CHUNK_ADDR(addr) (reinterpret_cast<PmChunk<KEY, VALUE> *>(addr))
#define GETP_CHUNKLIST_ADDR(addr) (reinterpret_cast<PmChunkList<KEY, VALUE> *>(addr))
#define GETP_UINT8(addr) (reinterpret_cast<uint8_t *>(pmem_start_addr + (addr)))
#define GETP_CHAR(addr) (reinterpret_cast<char *>(addr))
#define GETP_PAIR(addr) (reinterpret_cast<Pair_t<KEY, VALUE> *>(addr))
#define GET_UINT64(addr) (reinterpret_cast<uint64_t>(addr))

    std::atomic<uint64_t> hlsh_write_count{0};

    inline size_t Round2StripeSize(size_t size) {
        auto s = size % kStripeSize;
        if (s) {
            return ((size / kStripeSize) + 1) * kStripeSize;
        }
        return size;
    }

    struct PmOffset
    {
        uint64_t spos : 8;
        uint64_t chunk_addr: 44;
        uint64_t offset : 12;

        PmOffset() : chunk_addr(0), offset(0) {}
        PmOffset(uint64_t c, uint64_t o) : chunk_addr(c), offset(o) {}
        bool operator==(const PmOffset &other) const
        {
            return chunk_addr == other.chunk_addr &&
                   offset == other.offset;
        }

        bool operator!=(const PmOffset &other) const
        {
            return !(chunk_addr == other.chunk_addr &&
                     offset == other.offset);
        }

        PmOffset& operator=(const PmOffset& other){
            // s: don't overwrite spos
            chunk_addr = other.chunk_addr;
            offset = other.offset;
            return *this;
        }
        void InitValue()
        {
            chunk_addr = 0;
            offset = 0;
        }

        void Set(uint64_t c, uint64_t o)
        {
            chunk_addr = c;
            offset = o;
        }

        inline uint64_t GetValue()
        {
            return pmem_start_addr + chunk_addr + offset;
        }

        inline uint64_t GetChunk()
        {
            return pmem_start_addr + chunk_addr;
        }
    } PACKED;

    PmOffset PO_NULL(0, 0); // NULL offset for PM
    PmOffset PO_DEFAULT(1, 0); // NULL offset for PM

    thread_local PmOffset tl_value = PO_NULL;

    enum CHUNK_FLAG
    {
        FREE_CHUNK,
        SERVICE_CHUNK, //inserting chunk
        FULL_CHUNK
    };

    union FlagOffset {
      struct {
          uint64_t flag : 3;    // reference chunk_flag
          uint64_t offset : 53; // offset in chunk
      } fo;
      uint64_t entire;

      FlagOffset(uint64_t e)
      {
          entire = e;
      }
    };

    template <class KEY, class VALUE>
    struct PmChunk {
      FlagOffset foffset;  // offset
      uint64_t free_size;
      uint64_t chunk_addr; // relative addr
      uint64_t next_chunk; //relative addr
      // metadata for reclaim
      uint64_t reclaim_pos;  //reclaim_pos
      uint64_t dst_chunk1;  // 48(chunk_id) + 16(offset)
      uint64_t dst_chunk2;

      inline void Recovery(size_t chunk_addr, HLSH<KEY, VALUE> *index)
      {
          // s1: caclulate chunk addr for each chunk
          auto start_addr = pmem_start_addr + chunk_addr;
          auto of = sizeof(PmChunk<KEY, VALUE>);
          // s2: traverse and insert valid key-value into index
          auto p = GETP_PAIR(start_addr);
          while (!p->IsEnd())
          {
              if (p->IsValid())
              {
                  tl_value.Set(chunk_addr, of);
                  index->Insert(p, 0, true);
              }
              of += p->size();
              p = GETP_PAIR(start_addr + of);
          }
          // s3: set flag
          foffset.fo.flag = CHUNK_FLAG::FULL_CHUNK;
          foffset.fo.offset = of;
          clwb_sfence(&foffset, sizeof(FlagOffset));
      }
    };

    /*pm management for per list*/
    template <class KEY, class VALUE>
    struct PmChunkList {
        uint64_t chunk_list_addr; //physic addr: metadata area
        uint64_t list_id; //id for list
        uint64_t next; // head for service_chunk_lists
        uint64_t cur;  // tail for service_chunk_lists
        uint64_t free; // head for free_chunk_lists
        uint64_t tail; // tail for free_chunk_lists
        uint64_t previous_chunk_in_reclaim; // last chunk that searched in service chunk list
        uint64_t victim_chunk; // victim chunk

        uint64_t dst_chunk; // destination_chunk that used to store the reclaimed kv pairs
        HLSH<KEY,VALUE>* index; //Index on DRAM
        uint64_t depth; // depth for hash table on DRAM
        uint64_t total_chunk; // total chunk number
        uint64_t used_chunk;  // has used chunk number, 49(start reclaim), 50(start get new chunk)
        uint64_t start_chunk;// start chunk id
        bool terminate_reclaim;
        bool is_reclaim;

        /*first create*/
        void Create(uint64_t physic_addr, uint64_t id,
                    uint64_t stripe_num, HLSH<KEY, VALUE> *_index)
        {
            // s1: initial metadata for chunk list head
            chunk_list_addr = physic_addr;
            list_id = id;
            next = kMaxIntValue;
            cur = kMaxIntValue;
            free = chunk_list_addr + kStripeSize;
            tail = chunk_list_addr + (stripe_num - 1) * kStripeSize;
            previous_chunk_in_reclaim = kMaxIntValue;
            victim_chunk = kMaxIntValue;
            dst_chunk = kMaxIntValue;
            index = _index;
            total_chunk = stripe_num;
            used_chunk = 1;
            terminate_reclaim = true;
            is_reclaim = false;
            // s2: initial metadata for each chunk
            for (size_t i = 1; i < stripe_num; i++)
            {
                // s2.1: get chunk address
                auto chunk_addr = chunk_list_addr + i * kStripeSize;
                auto chunk = GETP_CHUNK(chunk_addr);
                // s2.2: set chunk metadata
                chunk->foffset.fo.flag = CHUNK_FLAG::FREE_CHUNK;
                chunk->foffset.fo.offset = sizeof(PmChunk<KEY,VALUE>);
                chunk->chunk_addr = chunk_addr;
                chunk->free_size = 0;
                chunk->reclaim_pos = sizeof(PmChunk<KEY, VALUE>);
                chunk->dst_chunk1 = kMaxIntValue;
                chunk->dst_chunk2 = kMaxIntValue;
                if ((stripe_num - 1) != i)
                {
                    chunk->next_chunk = chunk_addr + kStripeSize;
                }
                else
                {
                    chunk->next_chunk = kMaxIntValue;
                }
                // s2.3: persist chunk metadata
                clwb_sfence(chunk, sizeof(PmChunk<KEY,VALUE>));
            }
            // s3: persist chunk list head
            clwb_sfence(this, sizeof(PmChunkList<KEY, VALUE>));
        }

        void Print()
        {
            auto k = free;
            do
            {
                auto chunk = GETP_CHUNK(k);
                uint64_t chunk_id = (free - chunk_list_addr) / kStripeSize;
                printf("chunk_id: %lu\n", chunk_id);
                k = chunk->next_chunk;
            } while (k != kMaxIntValue);
        }

        /* open*/
        void Open(uint64_t physic_addr, HLSH<KEY, VALUE> *_index)
        {
            chunk_list_addr = physic_addr;
            index = _index;
            start_chunk = next;
            terminate_reclaim = true;
            is_reclaim = false;
        }

        inline void RecoverAllocate()
        {
            /* s: allocate for normal process */
            if (cur != kMaxIntValue)
            {
                auto cur_chunk = GETP_CHUNK(cur);
                if (cur_chunk->next_chunk != kMaxIntValue)
                {
                    // s: recovery cur and free
                    bool CurEqFree = false;
                    if (cur == free)
                    {
                        // s3.2.4: move free to next chunk
                        free = cur_chunk->next_chunk;
                        clwb_sfence(this, CACHE_LINE_SIZE);
                        CurEqFree = true;
                    }
                    cur_chunk->next_chunk = kMaxIntValue;
                    clwb_sfence(&cur_chunk->next_chunk, sizeof(uint64_t));
                    // s: recover used_chunk
                    if (CurEqFree ||
                        ((!CurEqFree) && CHECK_BITL(used_chunk, 50)))
                    {
                        used_chunk = UNSET_BITL(used_chunk, 50) + 1;
                        clwb_sfence(&used_chunk, sizeof(uint64_t));
                    }
                    // s: recover dst_chunk
                    if (dst_chunk == free)
                    {
                        dst_chunk = kMaxIntValue;
                        clwb_sfence(&dst_chunk, sizeof(uint64_t));
                    }
                }
                else
                {
                    // s: increase used chunk number
                    if (CHECK_BITL(used_chunk, 50))
                    {
                        used_chunk = UNSET_BITL(used_chunk, 50) + 1;
                        clwb_sfence(&used_chunk, sizeof(uint64_t));
                    }
                }
            }
            else
            {
                // s: crash from start
                next = kMaxIntValue;
                clwb_sfence(&next, sizeof(uint64_t));
            }
        }

        inline void RecoverReclaim()
        {
            if (victim_chunk == kMaxIntValue)
                return;
            auto victim_chunk_addr = pmem_start_addr + victim_chunk;
            auto chunk = GETP_CHUNK_ADDR(victim_chunk_addr); // victim chunk
            if (chunk->reclaim_pos != kMaxIntValue)
            {
                // s1: pairs in victim chunks don't finish reclaiming
                if (chunk->dst_chunk1 == kMaxIntValue)
                    return;
                auto chunk_num = chunk->dst_chunk1;
                if (chunk->dst_chunk2 != kMaxIntValue){
                    // s: check dst_chunk2 is valid
                    auto dchunk = GETP_CHUNK(chunk->dst_chunk2 >> 16); // victim chunk
                    auto dchunk_start = GET_UINT64(dchunk) + sizeof(PmChunk<KEY, VALUE>);
                    auto start_pair = GETP_PAIR(dchunk_start);
                    if (!start_pair->IsEnd())
                        chunk_num = chunk->dst_chunk2;
                }
                auto dst_chunk_addr = pmem_start_addr + (chunk_num >> 16);
                auto dchunk = GETP_CHUNK_ADDR(dst_chunk_addr); // victim chunk
                auto doffset = MASKL(chunk_num, 16);
                auto dst_start = dst_chunk_addr + doffset;
                auto dst = GETP_PAIR(dst_start);
                auto next_dst = GETP_PAIR(dst_start + dst->size());
                if (!dst->IsEnd())
                {
                    // s: find last pair in dst chunk
                    while (!next_dst->IsEnd())
                    {
                        doffset += dst->size();
                        dst_start += dst->size();
                        dst = GETP_PAIR(dst_start);
                        next_dst = GETP_PAIR(dst_start + dst->size());
                    }
                }
                auto voffset = sizeof(PmChunk<KEY, VALUE>);
                auto victim_start = victim_chunk_addr + voffset;
                auto src = GETP_PAIR(victim_start);
                if (!dst->IsEnd())
                {
                    // s: find corresponding pair in victim chunk
                    while ((dst->cmp_key(src)) &&
                           (!src->IsEnd()))
                    {
                        voffset += src->size();
                        victim_start += src->size();
                        src = GETP_PAIR(victim_start);
                    }
                }
                if (!dst->IsEnd())
                {
                    // s: update offset to next position for moving
                    doffset += dst->size();
                    dst_start += dst->size();
                }
                if (!src->IsEnd())
                {
                    voffset += src->size();
                    victim_start += src->size();
                }
                printf("reclaim crash, list_id: %lu, voffset: %lu, doffset: %lu\n",
                       list_id, voffset, doffset);
                // s: restart move process
                Move(chunk, victim_start, voffset, dchunk, dst_start, doffset);
                return;
            }
            else
            {
                // s2: victim chunk don't link to free list
                bool is_reclaim = false;
                // s2.1: remove from service chunk list
                if (previous_chunk_in_reclaim != kMaxIntValue)
                {
                    auto pchunk = GETP_CHUNK(previous_chunk_in_reclaim);
                    if (pchunk->next_chunk == victim_chunk)
                    {
                        pchunk->next_chunk = chunk->next_chunk;
                        clwb_sfence(&pchunk->next_chunk, sizeof(uint64_t));
                        is_reclaim = true;
                    }
                }
                else
                {
                    if (next == victim_chunk)
                    {
                        next = chunk->next_chunk;
                        clwb_sfence(&next, sizeof(uint64_t));
                        is_reclaim = true;
                    }
                }
                // s2.2: append to the tail of free list
                auto tchunk = GETP_CHUNK(tail);
                if (tchunk->next_chunk != victim_chunk)
                {
                    tchunk->next_chunk = victim_chunk;
                    clwb_sfence(&tchunk->next_chunk, sizeof(uint64_t));
                    is_reclaim = true;
                }
                if (tail != victim_chunk)
                {
                    // s: move tail to the victim chunk and set victim chunk as the last chunk
                    tail = victim_chunk;
                    clwb_sfence(&tail, sizeof(uint64_t));
                    is_reclaim = true;
                }
                // s2.3: update the flag to indicate free chunk
                chunk->foffset.fo.flag = CHUNK_FLAG::FREE_CHUNK;
                chunk->foffset.fo.offset = sizeof(PmChunk<KEY, VALUE>);
                chunk->free_size = 0;
                chunk->dst_chunk2 = kMaxIntValue;
                chunk->dst_chunk1 = kMaxIntValue;
                chunk->next_chunk = kMaxIntValue;
                // s: persist all metadata in one cacheline
                clwb_sfence(chunk, CACHE_LINE_SIZE);
                // s: decrease the used chunk num
                if (CHECK_BITL(used_chunk, 49) || is_reclaim)
                {
                    auto uc = UNSET_BITL(used_chunk, 49) - 1;
                    STORE(&used_chunk, uc);
                    clwb_sfence(&used_chunk, sizeof(uint64_t));
                }
            }
        }

        inline void RecoverChunk(size_t chunk_addr)
        {
            // s: recovery chunk with chunk adddr
            auto chunk = GETP_CHUNK(chunk_addr);
            chunk->Recovery(chunk_addr, index);
        }

        /*recovery for current chunk list*/
        void RecoverIndex()
        {
            // s: multiple thread travese the chunk list
            auto sc = LOAD(&start_chunk);
            while (sc != kMaxIntValue)
            {
                auto chunk = GETP_CHUNK(sc);
                if (CAS(&start_chunk, &sc, chunk->next_chunk))
                {
                    chunk->Recovery(sc, index);
                    sc = LOAD(&start_chunk);
                }
            }
        }

        /* reclaim process for current chunk list*/ 
        void Reclaim() {
            printf("list: %lu, start reclaim\n", list_id);
            STORE(&terminate_reclaim, false);
            do
            {
                // s1: get victim chunk for specific chunk list
                GetReclaimChunk();
                // s: terminate reclaim
                if (LOAD(&terminate_reclaim))
                    break;
                // s2: move from victim chunk to destination chunk
                Move2CurrentChunk();
                // s: terminate reclaim if avalid chunk exceed threshold
                if (used_chunk < (0.2 * total_chunk))
                    break;
            } while (1);
            // s: indicate reclaim finish
            STORE(&is_reclaim, false);
            printf("list: %lu, end reclaim, used_chunk: %lu, total_chunk: %lu\n",
                   list_id, used_chunk, total_chunk);
        }

        inline void GetReclaimChunk()
        { 
            auto reclaim_threshold = kMaxReclaimThreshold;
            // s1: get start chunk for traverse by prechuk_of_victim
            if (previous_chunk_in_reclaim == kMaxIntValue)
            {
                victim_chunk = next;
            }
            else
            {
                auto pre_chunk = GETP_CHUNK(previous_chunk_in_reclaim);
                victim_chunk = pre_chunk->next_chunk;
            }
            uint64_t version_pre_chunk = victim_chunk;
            bool is_first = true;
            do
            {
                auto c = LOAD(&cur);
                while (victim_chunk == cur)
                {
                    // s: restart from the list head
                    previous_chunk_in_reclaim = kMaxIntValue;
                    victim_chunk = next;
                    version_pre_chunk = next;
                    is_first = true;
                    c = LOAD(&cur);
                }
                // s: check wheterh chunk can be reclaimed
                auto chunk = GETP_CHUNK(victim_chunk);
                auto f = LOAD(&chunk->foffset.entire);
                FlagOffset fo(f);
                auto free_size = (kChunkSize - fo.fo.offset) +
                                 chunk->free_size;
                if ((fo.fo.flag == CHUNK_FLAG::FULL_CHUNK) &&
                    (free_size > reclaim_threshold))
                {
                    break;
                }
                // s: check whether need to degrade reclaim threshold
                if (victim_chunk == version_pre_chunk)
                {
                    if (!is_first)
                    {
                        // s: search one cycle with no suitable chunk
                        reclaim_threshold = reclaim_threshold / 2;
                        if (reclaim_threshold < kMinReclaimThreshold)
                        {
                            STORE(&terminate_reclaim, true);
                            printf("Warning: list_id %lu no space can be reclaimed!\n", list_id);
                        }
                    }else{
                        is_first = false;
                    }
                }
                // s: check next chunk
                previous_chunk_in_reclaim = victim_chunk;
                victim_chunk = chunk->next_chunk;
                // s: check whether need to terminate reclaim
                if (LOAD(&terminate_reclaim))
                {
                    break;
                }
            } while (1);
            clwb_sfence(&previous_chunk_in_reclaim, sizeof(uint64_t) * 2);
        }

        /*garbage:  merge fragmented data*/
        inline void Move2CurrentChunk()
        {
            if (victim_chunk == kMaxIntValue)
                return;
            // s: obtain destination chunk 
            auto dchunk = GetNewChunk(true);
            auto doffset = dchunk->foffset.fo.offset;
            auto dst_start = GET_UINT64(dchunk) + doffset;
            // s: obtain victim chunk
            auto chunk_addr = pmem_start_addr + victim_chunk;
            auto chunk = GETP_CHUNK_ADDR(chunk_addr); // victim chunk
            auto voffset = sizeof(PmChunk<KEY, VALUE>);
            auto victim_start = chunk_addr + voffset;
            // s: set dst_chunk1 for victim chunk
            chunk->dst_chunk1 = (dchunk->chunk_addr << 16) + doffset;
            clwb_sfence(&chunk->dst_chunk1, sizeof(uint64_t));
            // s: start move data
            Move(chunk, victim_start, voffset, dchunk, dst_start, doffset);
        }

        inline void Move(PmChunk<KEY, VALUE> *chunk, size_t victim_start,
                         size_t voffset, PmChunk<KEY, VALUE> *dchunk,
                         size_t dst_start, size_t doffset)
        {
            // s1: move valid data from victim chunk to destination chunk
            auto src = GETP_PAIR(victim_start);
            auto dst = GETP_CHAR(dst_start);
            while (!src->IsEnd())
            {
                auto kvsize = src->size();
                if (kChunkSize < (doffset + kvsize + sizeof(uint8_t)))
                {
                    /* s: destination chunk has full, flush destination chunk,
                     * and switch to next chunk*/
                    dchunk->foffset.fo.offset = doffset;
                    dchunk->foffset.fo.flag = CHUNK_FLAG::FULL_CHUNK;
                    clwb_sfence(&dchunk->foffset, sizeof(FlagOffset));
                    // s: set dst_chunk as kMaxIntValue
                    dst_chunk = kMaxIntValue;
                    clwb_sfence(&dst_chunk, sizeof(uint64_t));
                    // s: get new destination chunk 
                    dchunk = GetNewChunk(true);
                    // s: set dst_chunk as dst_chunk2
                    chunk->dst_chunk2 = (dchunk->chunk_addr << 16) +
                                        dchunk->foffset.fo.offset;
                    clwb_sfence(&chunk->dst_chunk2, sizeof(uint64_t));
                    // s: reset other variable
                    doffset = dchunk->foffset.fo.offset;
                    dst_start = GET_UINT64(dchunk) + doffset;
                    dst = GETP_CHAR(dst_start);
                }
                if (src->IsValid())
                {
                    // s: store into destination chunk
                    src->store_persist(dst);
                    // s: update index on DRAM
                    auto src_chunk_addr = GET_UINT64(chunk) - pmem_start_addr;
                    PmOffset old_value(src_chunk_addr, voffset);
                    auto dst_chunk_addr = GET_UINT64(dchunk) - pmem_start_addr;
                    PmOffset new_value(dst_chunk_addr, doffset);
                    index->UpdateForReclaim(src, old_value, new_value);
                    // s: next position in destination chunk
                    doffset += kvsize;
                    dst += kvsize;
                }
                voffset += kvsize;
                victim_start += kvsize;
                src = GETP_PAIR(victim_start);
            }

            // s: persist dst chunk metadata
            dchunk->foffset.fo.offset = doffset;
            clwb_sfence(&dchunk->foffset, sizeof(uint64_t));
            // s: indicate all valid pairs has been reclaimed in victim chunk
            chunk->reclaim_pos = kMaxIntValue;
            clwb_sfence(&chunk->reclaim_pos, sizeof(uint64_t));
            // s: move victim chunk to free lsit
            MoveVictimToFree(chunk);
        }

        inline void MoveVictimToFree(PmChunk<KEY, VALUE> *chunk)
        {
            std::lock_guard<std::mutex> lock(list_lock[list_id]);
            // s: set bit to indicate that start reclaim process
            used_chunk = SET_BITL(used_chunk, 49);
            // s1: separate victim chunk from service chunk list
            if (previous_chunk_in_reclaim != kMaxIntValue)
            {
                auto pchunk = GETP_CHUNK(previous_chunk_in_reclaim);
                pchunk->next_chunk = chunk->next_chunk;
                clwb_sfence(&pchunk->next_chunk, sizeof(uint64_t));
            }
            else
            {
                next = chunk->next_chunk;
                clwb_sfence(&next, sizeof(uint64_t));
            }
            // s2: append to the tail of free list
            auto tchunk = GETP_CHUNK(tail); // destination chunk
            tchunk->next_chunk = victim_chunk;
            clwb_sfence(&tchunk->next_chunk, sizeof(uint64_t));
            // s: move tail to the victim chunk and set victim chunk as the last chunk
            tail = victim_chunk;
            clwb_sfence(&tail, sizeof(uint64_t));
            // s3: update the flag to indicate free chunk
            chunk->foffset.fo.flag = CHUNK_FLAG::FREE_CHUNK;
            chunk->foffset.fo.offset = sizeof(PmChunk<KEY, VALUE>);
            chunk->free_size = 0;
            chunk->dst_chunk2 = kMaxIntValue;
            chunk->dst_chunk1 = kMaxIntValue;
            chunk->next_chunk = kMaxIntValue;
            // s: persist all metadata in one cacheline
            clwb_sfence(chunk, CACHE_LINE_SIZE);
            // s4: decrease the used chunk num
            used_chunk = UNSET_BITL(used_chunk, 49) - 1;
            clwb_sfence(&used_chunk, sizeof(uint64_t));
        }



        /*Get new chunk: relcaim thread or worker thread*/
        PmChunk<KEY, VALUE> *GetNewChunk(bool is_for_reclaim = false)
        {
            std::lock_guard<std::mutex> lock(list_lock[list_id]);
            if (is_for_reclaim)
            {
                // s1: return reclaim chunk for reclaim thread directly
                //  if dst chunk is avaiable
                if (dst_chunk != kMaxIntValue)
                {
                    auto vchunk = GETP_CHUNK(dst_chunk);
                    return vchunk;
                }
            }
            if (((total_chunk - used_chunk) <= 3) &&
                (!is_for_reclaim))
            {
                // s: avoid no empty chunk for reclaim
                return nullptr;
            }
            // s: get a new empty chunk
            auto free_chunk_addr = LOAD(&free);
            auto chunk = GETP_CHUNK(free_chunk_addr);
            chunk->foffset.fo.flag = CHUNK_FLAG::SERVICE_CHUNK;
            chunk->foffset.fo.offset = sizeof(PmChunk<KEY, VALUE>);
            chunk->reclaim_pos = sizeof(PmChunk<KEY, VALUE>);
            auto end_flag = GETP_UINT8(free_chunk_addr + sizeof(PmChunk<KEY, VALUE>));
            *(end_flag) = kInvalidPair;
            clwb_sfence(&chunk->foffset, kCacheLineSize);
            // s3: link chunk to sevices chunk list or free chunk list
            // s3.1.1: set destination chunk that used to store the reclaimed kv pairs
            clwb_sfence(end_flag, sizeof(uint8_t));
            if (is_for_reclaim)
            {
                dst_chunk = free_chunk_addr;
                clwb_sfence(&dst_chunk, sizeof(dst_chunk));
            }
            /* s: link new chunk to tail for worker thread */
            // s3.2.1: link new chunk to service chunk list
            if (kMaxIntValue == cur)
            {
                /* s: cur chunk is the head */
                // s: make next pointer pointing to new chunk
                next = free_chunk_addr;
                clwb_sfence(&next, sizeof(uint64_t));
            }
            else
            {
                /* s: cur chunk is not the head */
                // s3.2.1.3: make next pointer of cur chunk pointing to new chunk
                auto cur_chunk = GETP_CHUNK(cur);
                cur_chunk->next_chunk = free_chunk_addr;
                clwb_sfence(&chunk->next_chunk, sizeof(uint64_t));
            }
            // s3.2.3: make cur point to new chunk
            cur = free_chunk_addr;
            clwb_sfence(&cur, sizeof(uint64_t));
            // s: set allocate flag
            used_chunk = SET_BITL(used_chunk, 50);
            clwb_sfence(&used_chunk, sizeof(uint64_t));
            // s3.2.4: move free to next chunk
            free = chunk->next_chunk;
            clwb_sfence(&free, sizeof(uint64_t));
            // s3.2.5: separate new chunk from free list
            chunk->next_chunk = kMaxIntValue;
            clwb_sfence(&chunk->next_chunk, sizeof(uint64_t));
            // s: if avaliable chunk lower than threshold, reclaim process start
            used_chunk = UNSET_BITL(used_chunk, 50) + 1;
            clwb_sfence(&used_chunk, sizeof(uint64_t));
            if (used_chunk > (total_chunk * 0.8))
            {
                // s: start reclaim thread due to less empty chunk
                bool b = false;
                if (CAS(&is_reclaim, &b, true))
                {
                    std::thread(&PmChunkList::Reclaim, this).detach();
                }
            }
            else if (used_chunk < (0.2 * total_chunk))
            {
                // s: enough chunk, can teminate reclaim thread for performance
                STORE(&terminate_reclaim, true);
            }
            return chunk;
        }
    };
}
