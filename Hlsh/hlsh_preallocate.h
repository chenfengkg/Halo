#include "hlsh_bucket.h"
namespace HLSH_hashing
{

    thread_local void *tl_schunk = nullptr; // segchunk

    constexpr size_t kSegNumInOneChunk = 5;
    constexpr size_t kSegThreshold = 3;

    template <class KEY, class VALUE>
    struct Segment;

    template <class KEY, class VALUE>
    struct SegChunk
    {
        Segment<KEY, VALUE> schunk[kSegNumInOneChunk];

        void Init()
        {
            for (size_t i = 0; i < kSegNumInOneChunk; i++)
            {
                schunk[i].Init();
            }
        }
    };
    
    template <class KEY, class VALUE>
    struct SegChunkList
    {
        uint64_t count;
        SegChunk<KEY, VALUE> *cur;
        SegChunk<KEY, VALUE> *next;

        SegChunkList()
        {
            count = 0;
            cur = AllocateNewChunk();
            next = nullptr;
        }

        inline SegChunk<KEY, VALUE> *AllocateNewChunk()
        {
            SegChunk<KEY, VALUE> *sc = nullptr;
            if (!posix_memalign(reinterpret_cast<void **>(&sc), kCacheLineSize,
                                sizeof(SegChunk<KEY, VALUE>)))
            {
                sc->Init();
            }
            else
            {
                printf("allocate new segment failure!\n");
            }
            return sc;
        }

        Segment<KEY, VALUE> *Get()
        {
            Segment<KEY, VALUE> *s = nullptr;
            if (count < kSegNumInOneChunk)
            {
                s = &cur->schunk[count++];
            }
            else
            {
                cur = next;
                count = 0;
                s = &cur->schunk[count++];
                next = nullptr;
            }
            if (count > kSegThreshold)
            {
                if (!next)
                { 
                    // s: allocate new segchunk
                    next = AllocateNewChunk();
                }
            }
            return s;
        }
    };

}