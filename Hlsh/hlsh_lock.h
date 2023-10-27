#include "hlsh_pair.h"

namespace HLSH_hashing
{
    const uint16_t lockSet16 = ((uint16_t)1 << 15);
    const uint16_t lockMask16 = lockSet16 - 1;

    struct VersionLock16
    {
        uint16_t version_lock;

        VersionLock16() { version_lock = 0; }

        inline void Init() { version_lock = 0; }

        inline uint16_t GetVersion()
        {
            auto old_value = LOAD(&version_lock);
            while (old_value & lockSet16) {
                old_value = LOAD(&version_lock);
            }
            return old_value;
        }

        // test whether the version has change, if change, return true
        inline bool VersionIsChanged(uint16_t old_version)
        {
            auto value = LOAD(&version_lock);
            return (old_version != value);
        }

        inline bool VersionIsChanged2(uint16_t old_version)
        {
            auto value = LOAD(&version_lock);
            return (old_version != (value & lockMask16));
        }

        inline void GetLock()
        {
            uint16_t old_value = LOAD(&version_lock) & lockMask16;
            uint16_t new_value = old_value | lockSet16;
            while (!CAS(&version_lock, &old_value, new_value))
            {
                while (old_value & lockSet16)
                {
                    _mm_pause();
                    old_value = LOAD(&version_lock);
                }
                new_value = old_value | lockSet16;
            }
        }

        inline bool TryGetLock() {
            auto v = LOAD(&version_lock);
            if (v & lockSet16)
            {
                return false;
            }
            auto old_value = v & lockMask16;
            auto new_value = v | lockSet16;
            return CAS(&version_lock, &old_value, new_value);
        }

        inline void ReleaseLock()
        {
            auto v = version_lock;
            STORE(&version_lock, (v + 1) & lockMask16);
        }

        /*if the lock is set, return true*/
        inline bool TestLockSet(uint64_t &version) {
            version = LOAD(&version_lock);
            return (version & lockSet16) != 0;
        }

    } PACKED;

    struct Lock16
    {
        uint16_t lock;

        Lock16() { lock = 0; }

        inline void Init() { lock = 0; }

        inline void GetLock() {
            uint16_t new_value = 0;
            uint16_t old_value = 0;
            do {
                while (true) {
                    old_value = LOAD(&lock);
                    if (!(old_value & lockSet16)) break;
                }
                new_value = old_value | lockSet16;
            } while (!CAS(&lock, &old_value, new_value));
        }


        inline void ReleaseLock() {
            STORE(&lock, 0);
        }

    } PACKED;

    const uint64_t lockSet64 = ((uint64_t)1 << 63);
    const uint64_t lockMask64 = lockSet64 - 1;

    struct VersionLock64
    {
        uint64_t version_lock;

        VersionLock64() { version_lock = 0; }

        inline void GetLock()
        {
            uint64_t new_value = 0;
            uint64_t old_value = 0;
            do
            {
                while (true)
                {
                    old_value = LOAD(&version_lock);
                    if (!(old_value & lockSet64))
                        break;
                }
                new_value = old_value | lockSet64;
            } while (!CAS(&version_lock, &old_value, new_value));
        }

        inline bool TryGetLock()
        {
            auto v = LOAD(&version_lock);
            if (v & lockSet64)
            {
                return false;
            }
            auto old_value = v & lockMask64;
            auto new_value = v | lockSet64;
            return CAS(&version_lock, &old_value, new_value);
        }

        inline void ReleaseLock()
        {
            auto v = version_lock;
            STORE(&version_lock, (v + 1) & lockMask64);
        }

        /*if the lock is set, return true*/
        inline bool TestLockSet(uint64_t &version)
        {
            version = LOAD(&version_lock);
            return (version & lockSet64) != 0;
        }

        // test whether the version has change, if change, return true
        inline bool TestLockVersionChange(uint64_t old_version)
        {
            auto value = LOAD(&version_lock);
            return (old_version != value);
        }
    } __attribute__((packed));

    struct Lock64
    {
        uint64_t lock; // the MSB is the lock bit; remaining bits are used as the counter

        Lock64() { lock = 0; }
        inline void Init() { lock = 0; }

        inline int TestLockSet(void)
        {
            uint64_t v = LOAD(&lock);
            return v & lockSet64;
        }

        inline void GetLock()
        {
            uint64_t new_value = 0;
            uint64_t old_value = 0;
            do
            {
                while (true)
                {
                    old_value = LOAD(&lock);
                    if (!(old_value & lockSet64))
                        break;
                }
                new_value = old_value | lockSet64;
            } while (!CAS(&lock, &old_value, new_value));
        }
        
        // just set the lock as 0
        void ReleaseLock()
        {
            STORE(&lock, 0);
        }
    } PACKED;

    const uint8_t lockSet8 = ((uint8_t)1 << 7);
    const uint8_t lockMask8 = lockSet8 - 1;

    struct Lock8
    {
        uint8_t lock;

        Lock8() { lock = 0; }
        inline void Init() { lock = 0; }

        inline void GetLock()
        {
            uint8_t new_value = 0;
            uint8_t old_value = 0;
            do
            {
                while (true)
                {
                    old_value = LOAD(&lock);
                    if (!(old_value & lockSet8))
                        break;
                }
                new_value = old_value | lockSet8;
            } while (!CAS(&lock, &old_value, new_value));
        }

        inline bool TryGetLock()
        {
            auto old_value = LOAD(&lock);
            if (old_value & lockSet8)
            {
                return false;
            }
            old_value = old_value & lockMask8;
            uint8_t new_value = old_value | lockSet8;
            return CAS(&lock, &old_value, new_value);
        }

        inline void ReleaseLock()
        {
            STORE(&lock, 0);
        }

        void reset() { lock = 0; }

    } PACKED;

}
