#include "hlsh_index_persist.h"
#include <queue>
#include <future>

namespace HLSH_hashing
{
  constexpr size_t kSegNum = 80;
  constexpr size_t kUsedSegNum = 10;

  template <class KEY, class VALUE>
  struct BlockAllocator
  {
    Segment<KEY, VALUE> s[kSegNum];
    std::atomic<uint64_t> count;
    std::atomic<BlockAllocator<KEY, VALUE> *> next;

    BlockAllocator()
    {
      count.store(0, std::memory_order_relaxed);
      next.store(nullptr, std::memory_order_relaxed);
    }

    inline Segment<KEY, VALUE> *GetTable()
    {
      size_t c = count.fetch_add(1, std::memory_order_release);
      if (c >= kSegNum) return nullptr;
      return &s[c];
    }
  } ALIGNED(64);

  template <class KEY, class VALUE>
  struct SegmentAllocator
  {
    BlockAllocator<KEY, VALUE> *ba_head[kThreadNum];
    BlockAllocator<KEY, VALUE> *ba_end[kThreadNum];
    std::thread seg_thread;

    size_t MemoryUsage() const
    {
      size_t total_usage = sizeof(SegmentAllocator<KEY, VALUE>);
      // s1: travese each block allocator list
      for (size_t i = 0; i < kThreadNum; i++)
      {
        // s1.1: add size of each block allocator in current list
        auto b = ba_head[i];
        while (b)
        {
          total_usage += sizeof(BlockAllocator<KEY, VALUE>);
          b = b->next.load();
        }
      }
      return total_usage;
    }

    void PreAllocateSegment()
    {
      BlockAllocator<KEY, VALUE> *p = nullptr;
      // s2: preallocate memory for each list if used number exceed the threshold
      while (1)
      {
        for (size_t i = 0; i < kThreadNum; i++)
        {
          // s2.1: allocate memory for segment
          auto c = ba_end[i]->count.load(std::memory_order_acquire);
          auto n = ba_end[i]->next.load(std::memory_order_acquire);
          if ((!n) && (c > kUsedSegNum))
          {
            p = new BlockAllocator<KEY, VALUE>();
            ba_end[i]->next.store(p, std::memory_order_release);
          }
        }
      }
    }

    SegmentAllocator<KEY, VALUE>()
    {
#ifdef ENABLE_PREALLOC
      // s1: initial thread local segment list
      for (size_t i = 0; i < kThreadNum; i++)
      {
        ba_head[i] = new BlockAllocator<KEY, VALUE>();
        ba_end[i] = ba_head[i];
      }
      // s2: start preallocate thread
      seg_thread = std::thread(&SegmentAllocator<KEY, VALUE>::PreAllocateSegment, this);
#endif
    }

    inline Segment<KEY, VALUE> *Get(size_t thread_id)
    {
      // s1: get new segment from pre allocated memory
      auto tid = thread_id%kThreadNum;
      auto nb = ba_end[tid]->GetTable();
      if (nb)
        return nb;
      // s2: wait until new block allocator is allocated and then allocate
      // new segment
      auto n = ba_end[tid]->next.load(std::memory_order_acquire);
      while (!n)
      {
        n = ba_end[tid]->next.load(std::memory_order_acquire);
      };
      ba_end[tid] = n;
      return n->GetTable();
    }
  };

  constexpr size_t kDirArraySize = 2;

  template <class KEY, class VALUE>
  struct DirectoryAllocator
  {
    Directory<KEY, VALUE> *d[kDirArraySize];
    std::atomic<Directory<KEY, VALUE> *> new_dir;
    std::atomic<bool> is_empty[kDirArraySize];
    size_t depth;
    int index;
    int old_index;
    std::thread dir_thread;

    size_t MemoryUsage() const
    {
#ifdef ENABLE_PREALLOC
      // s1: caculate directory allocator size
      size_t total_usage = sizeof(DirectoryAllocator);
      // s2: caculate directory size in directory list
      for (size_t i = 0; i < kDirArraySize; i++)
      {
        total_usage += sizeof(Directory<KEY, VALUE>) +
                       d[i]->capacity * sizeof(Segment<KEY, VALUE> *);
      }
      // s3: caculate segmetn size fot the latest directory
      if (new_dir.load())
      {
        bool dup = false;
        for (size_t i = 0; i < kDirArraySize; i++)
        {
          if (new_dir == d[i])
            dup = true;
        }
        if (!dup)
          total_usage +=
              sizeof(Directory<KEY, VALUE>) +
              (new_dir.load())->capacity * sizeof(Segment<KEY, VALUE> *);
      }
      return total_usage;
#else
      return 0;
#endif
    }

    DirectoryAllocator(size_t _depth)
    {
#ifdef ENABLE_PREALLOC
      // s1: init variable and preallocate directory
      depth = _depth;
      index = -1;
      for (size_t i = 0; i < kDirArraySize; i++)
      {
        auto capacity = pow(2, depth);
        auto ret = posix_memalign(reinterpret_cast<void **>(&d[i]), kCacheLineSize,
                                  sizeof(Directory<KEY, VALUE>) + sizeof(Segment<KEY, VALUE> *) * capacity);
        if (ret)
        {
          printf("allocation directory failure, depth: %lu, code: %d\n", depth, ret);
        }
        memset(d[i], 0, sizeof(Directory<KEY, VALUE>) + sizeof(Segment<KEY, VALUE> *) * capacity);
        is_empty[i].store(false, std::memory_order_relaxed);
        depth++;
      }
      new_dir.store(nullptr, std::memory_order_release);
      // s2: start directory preallocate thread
      dir_thread = std::thread(&DirectoryAllocator<KEY, VALUE>::DirAlloc,
                               this, 34);
#endif
    }

    inline void WaitOldDir()
    {
      // s1: wait until old dir entries finish updating
      while ((new_dir.load(std::memory_order_acquire)) != nullptr)
        ;
    }

    inline void InsertNewDir(Directory<KEY, VALUE> *ndir)
    {
      // s1: use a background thread to deal with new dir entries update
      new_dir.store(ndir, std::memory_order_release);
      auto od = ndir->old_dir;
      for (size_t i = 0; i < od->capacity; i++)
      {
        if (!od->_[i])
        {
          printf("empty entry: %lu\n", i);
        }
      }
    }

    inline Directory<KEY, VALUE> *Get()
    {
      // s1: get new dirctory
      // s1.1: obtain index for next directory
      index = (index + 1) % kDirArraySize;
      // s1.2: wait until new dirctory is ready
      bool b = is_empty[index].load(std::memory_order_acquire);
      while (b)
      {
        b = is_empty[index].load(std::memory_order_acquire);
      } // wait new directory auto s = d[index];
      // s1.3: get new directory
      auto s = d[index];
      // s1.4: set flag is emtpy to preallocated directory
      is_empty[index].store(true, std::memory_order_release);
      return s;
    }

    void DirAlloc(size_t thread_id)
    {
      set_affinity(thread_id);
      Directory<KEY, VALUE> *nd = nullptr;
      Segment<KEY, VALUE> *t = nullptr;
      while (1)
      {
        // s1: udpate directory entries for the lastest directory
        nd = new_dir.load(std::memory_order_acquire);
        if (nd)
        {
          auto od = nd->old_dir;
          // s1.1: travese and help updating the old dir entries
          for (size_t i = 0; i < nd->capacity;)
          {
            // s1.1.1: check whether the entry is updated
            t = nd->_[i];
            if (t)
            {
              i++;
              continue;
            }
            // s1.1.2: get lock and update entries if the entry is not
            // updated
            t = od->_[i / 2];
            t->lock.GetLock();
            size_t chunk_size = pow(2, nd->global_depth - t->local_depth);
            // if (nd->_[i]) { t->lock.ReleaseLock(); i = i + chunk_size;
            // continue; }
            size_t start_pos = t->pattern << (nd->global_depth - t->local_depth);
            for (size_t j = 0; j < chunk_size; j++)
            {
              nd->_[start_pos + j] = t;
            }
            // s1.1.3: release lock and move to next entry
            t->lock.ReleaseLock();
            i = i + chunk_size;
          }
          // s1.2: reset new_dir indicate all entries have been
          // udpated
          new_dir.store(nullptr, std::memory_order_release);
        }

        // s2: preallocte new directory
        for (size_t i = 0; i < kDirArraySize; i++)
        {
          // s2.1: check whether directory is allocated in current
          // position
          bool b = is_empty[i].load(std::memory_order_acquire);
          if (!b)
            continue;
          // s2.2: preallocated directory if current position is empty
          depth++;
          size_t capacity = pow(2, depth);
          auto ret = posix_memalign(
              reinterpret_cast<void **>(&d[i]), kCacheLineSize,
              sizeof(Directory<KEY, VALUE>) +
                  sizeof(Segment<KEY, VALUE> *) * capacity);
          if (ret)
          {
            printf("allocation directory failure, depth: %lu, code: %d\n",
                   depth, ret);
          }
          memset(d[i], 0,
                 sizeof(Directory<KEY, VALUE>) +
                     sizeof(Segment<KEY, VALUE> *) * capacity);
          is_empty[i].store(false, std::memory_order_release);
        }
      }
    }
  };

}