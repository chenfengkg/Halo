#include "hlsh_pair.h"

namespace HLSH_hashing {

    union BucketVersionLock
    {
        struct  VLock {
            uint16_t lock : 1;
            uint16_t is_split_seg : 1;// is new segment: 0: false, 1: true
            uint16_t is_split : 1;// is split: 0: false,1: true;
            uint16_t version : 13; //version;
        } vlock;
        uint16_t pad;

        BucketVersionLock() :pad(0) {}
        BucketVersionLock(uint16_t lk) : pad(lk) {}
    };

    /* 8bit VersionLock: write with lock, read without lock*/
    struct BucketVLock {
        BucketVersionLock vlock;

        BucketVLock() {}

        /*InitalBucket: split bucket and is split*/
        inline void InitBucket() {
            vlock.vlock.is_split_seg = 1;
            vlock.vlock.is_split = 1;
        }

        inline uint64_t GetVersionWithoutLock() {
            BucketVersionLock l;
            uint16_t old_value;
            // s1: get vlock without lock
            do {
                old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
                l.pad = old_value;
            } while (l.vlock.lock);
            // s2: return
            return l.vlock.version;
        }

        inline void GetLock() {
            uint16_t old_value, new_value;
            BucketVersionLock l;
            do {
                // s1: get vlock without lock
                while (true) {
                    old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
                    l.pad = old_value;
                    if (!l.vlock.lock)  break;
                }
                // s2: set lock for vlock
                l.vlock.lock = 1;
                new_value = l.pad;
            } while (!CAS(&vlock.pad, &old_value, new_value));
        }


        inline bool TryGetLock() {
            // s1: get vlock
            uint16_t old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: return false if vlock is lock
            BucketVersionLock l(old_value);
            if (l.vlock.lock) { return false; }
            // s3: try to get lock
            l.vlock.lock = 1;
            uint16_t new_value = l.pad;
            return CAS(&vlock.pad, &old_value, new_value);
        }

        inline void ReleaseLock() {
            // s1: get vlock
            uint16_t old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: release lock and increase version
            BucketVersionLock l(old_value);
            l.vlock.lock = 0;
            l.vlock.version = (l.vlock.version + 1) & ((1 << 13) - 1);
            __atomic_store_n(&vlock.pad, l.pad, __ATOMIC_RELEASE);
        }

        inline void GetLockForSplit(bool is_split) {
            uint16_t old_value, new_value;
            BucketVersionLock l;
            do {
                // s1: get vlock without lock
                while (true) {
                    old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
                    l.pad = old_value;
                    if (!l.vlock.lock)  break;
                }
                // s2: set lock for vlock
                l.vlock.lock = 1;
                l.vlock.is_split_seg = is_split;
                l.vlock.is_split = 0;
                new_value = l.pad;
            } while (!CAS(&vlock.pad, &old_value, new_value));
        }

        inline void ReleaseLockForSplit() {
            // s1: get vlock
            uint16_t old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: release lock and increase version
            BucketVersionLock l(old_value);
            l.vlock.lock = 0;
            l.vlock.is_split = 1;
            l.vlock.version = (l.vlock.version + 1) & ((1 << 13) - 1);
            __atomic_store_n(&vlock.pad, l.pad, __ATOMIC_RELEASE);
        }

        /*if the lock is set, return true*/
        inline bool IsLockSet() {
            // s1: get vlock
            uint16_t old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: check wheteher lock is set
            BucketVersionLock l(old_value);
            return l.vlock.lock;
        }

        inline bool IsSplit() {
            // s1: get vlock
            uint16_t old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: check wheteher lock is set
            BucketVersionLock l(old_value);
            return l.vlock.is_split;
        }

        // test whether the version has change, if change, return true
        inline bool LockVersionIsChanged(uint16_t old_version) {
            // s1: get vlock
            auto old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: compare old version and new version
            BucketVersionLock l(old_value);
            return (old_version != l.vlock.version);
        }
        
        inline bool IsBucketSplit() {
            // s1: get vlock
            auto old_value = __atomic_load_n(&vlock.pad, __ATOMIC_ACQUIRE);
            // s2: return is split
            BucketVersionLock l(old_value);
            return l.vlock.is_split;
        }

    };

    const uint8_t lockSet = ((uint8_t)1 << 7);
    const uint8_t lockMask = ((uint8_t)1 << 7) - 1;
    struct VersionLock {
        uint8_t version_lock;

        VersionLock() { version_lock = 0; }

        inline void GetLock() {
            uint8_t new_value = 0;
            uint8_t old_value = 0;
            do {
                while (true) {
                    old_value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
                    if (!(old_value & lockSet))  break;
                }
                new_value = old_value | lockSet;
            } while (!CAS(&version_lock, &old_value, new_value));
        }

        inline bool TryGetLock() {
            uint8_t v = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
            if (v & lockSet) { return false; }
            auto old_value = v & lockMask;
            auto new_value = v | lockSet;
            return CAS(&version_lock, &old_value, new_value);
        }

        inline void ReleaseLock() {
            uint8_t v = version_lock;
            __atomic_store_n(&version_lock, (v + 1) & lockMask, __ATOMIC_RELEASE);
        }

        /*if the lock is set, return true*/
        inline bool TestLockSet(uint8_t& version) {
            version = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
            return (version & lockSet) != 0;
        }

        // test whether the version has change, if change, return true
        inline bool TestLockVersionChange(uint8_t old_version) {
            auto value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
            return (old_version != value);
        }
    };

    struct ReadShareLock {
        uint32_t lock;  // the MSB is the lock bit; remaining bits are used as the counter

        ReadShareLock() { lock = 0; }

        inline int Test_Lock_Set(void) {
            uint32_t v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
            return v & lockSet;
        }

        inline bool try_get_read_lock() {
            uint32_t old_value = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
            old_value = old_value & lockMask;
            auto new_value = (old_value + 1) & lockMask;
            return CAS(&lock, &old_value, new_value);
        }

        inline void release_read_lock() { SUB(&lock, 1); }

        void Lock() {
            uint32_t v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
            uint32_t old_value = v & lockMask;
            uint32_t new_value = old_value | lockSet;

            while (!CAS(&lock, &old_value, new_value)) {
                old_value = old_value & lockMask;
                new_value = old_value | lockSet;
            }

            // wait until the readers all exit the critical section
            v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
            while (v & lockMask) { v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE); }
        }

        // just set the lock as 0
        void Unlock() { __atomic_store_n(&lock, 0, __ATOMIC_RELEASE); }
    };

    constexpr uint8_t kLightLockSet = 128;
    constexpr uint8_t kLightLockMask = 127;
    struct LightLock {
        LightLock() { llock = 0; }

        inline void get_lock() {
            uint8_t new_value = 0;
            uint8_t old_value = 0;
            do {
                while (true) {
                    old_value = __atomic_load_n(&llock, __ATOMIC_ACQUIRE);
                    if (!(old_value & kLightLockSet)) break;
                }
                new_value = old_value | kLightLockSet;
            } while (!CAS(&llock, &old_value, new_value));
        }

        inline bool try_get_lock() {
            uint8_t old_value = __atomic_load_n(&llock, __ATOMIC_ACQUIRE); 
            if (old_value & kLightLockSet) { return false; }
            old_value = old_value & kLightLockMask;
            uint8_t new_value = old_value | kLightLockSet;
            return CAS(&llock, &old_value, new_value);
        }

        inline void release_lock() {
            uint8_t v = llock;
            __atomic_store_n(&llock, (v + 1) & kLightLockMask, __ATOMIC_RELEASE);
        }

        inline uint8_t get_version() {
            return __atomic_load_n(&llock, __ATOMIC_ACQUIRE);
        }

        inline bool test_lock_version_change(uint8_t old_version) {
            auto value = __atomic_load_n(&llock, __ATOMIC_ACQUIRE);
            return (old_version != value);
        }

        void reset() { llock = 0; }

        uint8_t llock;
    }__attribute__((packed));
}