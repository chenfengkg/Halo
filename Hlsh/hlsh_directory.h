#include "hlsh_segment.h"
namespace HLSH_hashing {
template <class KEY, class VALUE>
struct DirectoryAllocator;

template <class KEY, class VALUE>
struct PersistHash;

template <class KEY, class VALUE>
struct Directory {
  uint32_t global_depth;
  uint32_t version;
  size_t capacity;
  Directory<KEY, VALUE>* old_dir;
  PersistHash<KEY, VALUE> *ph;
  size_t interval; // interval with last persist
  bool entries_update; // indicate wheter directory entries is updated
  Segment<KEY, VALUE>* _[0];

  Directory(size_t cap, size_t _version, Directory<KEY, VALUE> *odir,
            size_t _interval, PersistHash<KEY, VALUE> *_ph,
            bool is_entries_udpate)
  {
    version = _version;
    global_depth = static_cast<size_t>(log2(cap));
    capacity = cap;
    old_dir = odir;
    ph = _ph;
    interval = _interval;
    STORE(&entries_update, is_entries_udpate);
    memset(_, 0, sizeof(Segment<KEY, VALUE>*) * capacity);
  }

  static void New(Directory<KEY, VALUE> **dir, size_t capacity,
                  size_t version, Directory<KEY, VALUE> *odir,
                  size_t _interval, PersistHash<KEY, VALUE> *_ph,
                  bool is_entries_udpate = false)
  {
    do
    {
      auto err = posix_memalign(reinterpret_cast<void **>(dir), kCacheLineSize,
                                sizeof(Directory<KEY, VALUE>) +
                                    sizeof(Segment<KEY, VALUE> *) * capacity);
      if (!err)
      {
        new (*dir) Directory(capacity, version, odir, _interval, _ph,
                             is_entries_udpate);
        return;
      }
      else
      {
        printf("Allocate directory failure: %d\n", err);
        fflush(stdout);
      }
    } while (1);
  }

  bool NeedPersist(){
    return false;
    // return true;
    // return interval > 100;
  }
};
}  // namespace HLSH_hashing