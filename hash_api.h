#include <string>
constexpr size_t pool_size = 1024ul * 1024ul * 1024ul * 128ul;
std::string index_pool_name = "/data/pmem0/";

#ifdef HLSHT
#define NONVAR 1
// #define VARVALUE 1
#include "Hlsh/hlsh_baseline.h"
using namespace HLSH_hashing;
class hash_api {
 public:
#ifdef NONVAR
  using HLSH_KEY = size_t;
  using HLSH_VALUE = size_t;
#elif VARVALUE
  using HLSH_KEY = size_t;
  using HLSH_VALUE = std::string;
#else
  using HLSH_KEY = std::string;
  using HLSH_VALUE = std::string;
#endif

  HLSH<HLSH_KEY, HLSH_VALUE> *t;
  hash_api(size_t sz = (1<<16))
  {
    PM_PATH = "/data/pmem0/";
    std::string hlsh = index_pool_name + "HLSH.data";
    size_t hlsh_pool_size = 32ul * 1024ul * 1024Ul * 1024ul;
    bool file_exist = false;
    if (FileExists(hlsh.c_str()))
    {
      file_exist = true; /* FileRemove(hlsh.c_str()); */
    }
    if (!file_exist)
      t = new HLSH<HLSH_KEY, HLSH_VALUE>(sz, hlsh.c_str(), hlsh_pool_size);
    else
      t = new HLSH<HLSH_KEY, HLSH_VALUE>(hlsh.c_str(), hlsh_pool_size);
  }
  ~hash_api() { delete t; }
  std::string hash_name() { return "HLSH"; };
  bool find(size_t key, void *p) {
    auto pt = reinterpret_cast<Pair_t<HLSH_KEY, HLSH_VALUE> *>(p);
#ifdef NONVAR
    pt->set_key(key);
#elif VARVALUE
    pt->set_key(key);
#else
    pt->set_key(reinterpret_cast<char *>(&key), 8);
#endif
    return t->Get(pt);
  }
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<HLSH_KEY, HLSH_VALUE> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<HLSH_KEY, HLSH_VALUE> p(key, value, value_len);
#else
    Pair_t<HLSH_KEY, HLSH_VALUE> p(reinterpret_cast<char *>(&key), 8, value,
                                   value_len);
#endif
    return t->Insert(&p, tid);
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<HLSH_KEY, HLSH_VALUE> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<HLSH_KEY, HLSH_VALUE> p(key, value, value_len);
#else
    Pair_t<HLSH_KEY, HLSH_VALUE> p(reinterpret_cast<char *>(&key), 8, value,
                                   value_len);
#endif
    return t->Update(&p, tid);
  }

  bool erase(size_t key, int tid = 0) {
    Pair_t<HLSH_KEY, HLSH_VALUE> p;
#ifdef NONVAR
    p.set_key(key);
#elif VARVALUE
    p.set_key(key);
#else
    p.set_key(reinterpret_cast<char *>(&key), 8);
#endif
    t->Delete(&p, tid);
    return true;
  }
  void wait() {}
  void space_consumption() {
    auto [used_dram, used_pmem] = t->SpaceConsumption();
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
  }
};
#endif

#ifdef HALOT
#define NONVAR 1
// #define VARVALUE 1
#include "Halo/Halo.hpp"
using namespace HALO;
class hash_api {
 public:
#ifdef NONVAR
  Halo<size_t, size_t> *t;
#elif VARVALUE
  Halo<size_t, std::string> *t;
#else
  Halo<std::string, std::string> *t;
#endif
  hash_api(size_t sz = 16 * 1024 * 1024)
  {
    PM_PATH = "/data/pmem0/hash/HaLo/";
#ifdef NONVAR
    t = new Halo<size_t, size_t>(sz);
#elif VARVALUE
    t = new Halo<size_t, std::string>(sz);
#else
    t = new Halo<std::string, std::string>(sz);
#endif
  }
  ~hash_api() { delete t; }
  std::string hash_name() { return "Halo"; };
  bool find(size_t key, void *p) {
#ifdef NONVAR
    auto pt = reinterpret_cast<Pair_t<size_t, size_t> *>(p);
    pt->set_key(key);
#elif VARVALUE
    auto pt = reinterpret_cast<Pair_t<size_t, std::string> *>(p);
    pt->set_key(key);
#else
    auto pt = reinterpret_cast<Pair_t<std::string, std::string> *>(p);
    pt->set_key(reinterpret_cast<char *>(&key), 8);
#endif
    return t->Get(pt);
  }
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<size_t, std::string> p(key, value, value_len);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&key), 8, value,
                                       value_len);
#endif
    return t->Insert(p, r);
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<size_t, std::string> p(key, value, value_len);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&key), 8, value,
                                       value_len);
#endif
    return t->Update(p, r);
  }

  bool erase(size_t key, int tid = 0) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p;
    p.set_key(key);
#elif VARVALUE
    Pair_t<size_t, std::string> p;
    p.set_key(key);
#else
    Pair_t<std::string, std::string> p;
    p.set_key(reinterpret_cast<char *>(&key), 8);
#endif
    t->Delete(p);
    return true;
  }
  void wait() { t->wait_all(); }
  void space_consumption() {
    auto [used_dram,used_pmem] = t->space_consumption();
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
     }
};
#endif

#ifdef CCEHT
#include "third/CCEH/CCEH_baseline.h"
class hash_api {
 public:
  cceh::CCEH<size_t> *cceh;
  hash_api(size_t sz = 1024 * 16) {
    bool file_exist = false;
    index_pool_name += "CCEH.data";
    if (FileExists(index_pool_name.c_str())) file_exist = true;
    Allocator::Initialize(index_pool_name.c_str(), pool_size);
    cceh = reinterpret_cast<cceh::CCEH<size_t> *>(
        Allocator::GetRoot(sizeof(cceh::CCEH<size_t>)));
    if (!file_exist) {
      new (cceh) cceh::CCEH<size_t>(sz, Allocator::Get()->pm_pool_);
    } else {
      new (cceh) cceh::CCEH<size_t>();
    }
  }
  ~hash_api() {
    printf("CCEH extra write, count: %lu, count1: %lu, total count: %lu\n",
           cceh_write_count.load(), cceh_write_count1.load(), cceh_write_count.load() + cceh_write_count1.load());
    Allocator::Close_pool();
  }
  std::string hash_name() { return "CCEH"; };
  bool find(size_t key) {
    cceh->Get(key);
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    cceh->Insert(key, DEFAULT);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    cceh->Delete(key);
    cceh->Insert(key, DEFAULT);
    return false;
  }
  bool erase(size_t key, int tid = 0) {
    cceh->Delete(key);
    return true;
  }
  void wait() {}
  void space_consumption()
  {
    auto [used_dram,used_pmem] = cceh->SpaceConsumption();
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
  }
};
#endif

#ifdef DASHT
#include "third/Dash/ex_finger.h"
class hash_api {
 public:
  extendible::Finger_EH<size_t> *dash;
  // sz is the number of segment.
  hash_api(size_t sz = 1024 * 16) {
    bool file_exist = false;
    index_pool_name += "DASH.data";

    if (FileExists(index_pool_name.c_str())) file_exist = true;
    Allocator::Initialize(index_pool_name.c_str(), pool_size);
    dash = reinterpret_cast<extendible::Finger_EH<size_t> *>(
        Allocator::GetRoot(sizeof(extendible::Finger_EH<size_t>)));
    if (!file_exist) {
      new (dash) extendible::Finger_EH<size_t>(sz, Allocator::Get()->pm_pool_);
    } else {
      new (dash) extendible::Finger_EH<size_t>();
    }
  }
  ~hash_api()
  {
    printf("extra_write, count: %lu, count1: %lu, total_count: %lu\n",
           extendible::dash_write_count.load(), extendible::dash_write_count1.load(),
           extendible::dash_write_count.load() + extendible::dash_write_count1.load());
    Allocator::Close_pool();
  }
  std::string hash_name() { return "Dash"; };
  bool find(size_t key) {
    dash->Get(key);
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    dash->Insert(key, DEFAULT);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    dash->Delete(key);
    dash->Insert(key, DEFAULT);
    return false;
  }

  bool erase(size_t key, int tid = 0) {
    dash->Delete(key);
    return true;
  }
  void wait() {}
  void space_consumption() {
    auto [used_dram, used_pmem] = dash->Space_Consumption();
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
  }
};
#endif

#ifdef CLEVELT
#include <filesystem>
// clang-format off
#include "third/CLevel/libpmemobj++/make_persistent.hpp"
#include "third/CLevel/libpmemobj++/p.hpp"
#include "third/CLevel/libpmemobj++/persistent_ptr.hpp"
#include "third/CLevel/libpmemobj++/pool.hpp"
#include "third/CLevel/src/libpmemobj_cpp_examples_common.hpp"
#include "third/CLevel/libpmemobj++/experimental/clevel_hash.hpp"
// clang-format on
#define LAYOUT "clevel_hash"
namespace nvobj = pmem::obj;
class hash_api {
  typedef nvobj::experimental::clevel_hash<nvobj::p<uint64_t>,
                                           nvobj::p<uint64_t>>
      persistent_map_type;

  struct root {
    nvobj::persistent_ptr<persistent_map_type> cons;
  };
  nvobj::pool<root> pop;
  nvobj::persistent_ptr<persistent_map_type> map;

 public:
  hash_api(size_t sz = 1024 * 16, int tnum = 32) {
    const char *path = "/data/pmem0/pmem.data";
    if (!std::filesystem::exists(path))
      pop =
          nvobj::pool<root>::create(path, LAYOUT, pool_size, S_IWUSR | S_IRUSR);
    else
      pop = nvobj::pool<root>::open(path, LAYOUT);
    auto proot = pop.root();
    nvobj::transaction::manual tx(pop);
    proot->cons = nvobj::make_persistent<persistent_map_type>();
    proot->cons->set_thread_num(tnum);
    map = proot->cons;
    nvobj::transaction::commit();
  }
  ~hash_api()
  {
    pop.close();
  }
  std::string hash_name() { return "CLevel"; };
  bool find(size_t key) {
    pmem::obj::p<uint64_t> k = key;
    auto r = map->search(persistent_map_type::key_type(k));
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    auto rr = map->insert(persistent_map_type::value_type(key, key), tid);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    auto rr = map->update(persistent_map_type::value_type(key, key + 1), tid);
    return true;
  }
  bool erase(size_t key, int tid) {
    auto r = map->erase(persistent_map_type::key_type(key), tid);
    return true;
  }
  void wait() {}
  void space_consumption() {}
};

#endif

#ifdef PCLHTT
#include "third/PCLHT/clht_lb_res.h"
class hash_api {
 public:
  PCLHT::clht_t *iclht;
  hash_api(size_t sz = 1024 * 1024 * 16) {
    sz = log2(sz / ENTRIES_PER_BUCKET);
    sz = pow(2, sz);
    iclht = PCLHT::clht_create(sz);
  }
  bool find(size_t key) {
    auto r = PCLHT::clht_get(iclht, key);
    return true;
  }
  std::string hash_name() { return "PCLHT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    PCLHT::clht_put(iclht, key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    // PCLHT::clht_put_replace(iclht, key, key);
    return false;
  }
  bool erase(size_t key, int tid) {
    PCLHT::clht_remove(iclht, key);
    return true;
  }
  void wait() {}
  void space_consumption() {}
};
#endif
#ifdef CLHTT
#include "third/CLHT/include/clht_lb_res.h"
class hash_api {
 public:
  clht_t *iclht;
  hash_api(size_t sz = 1024 * 1024 * 16) {
    sz = log2(sz / ENTRIES_PER_BUCKET);
    sz = pow(2, sz);
    iclht = clht_create(sz);
  }
  bool find(size_t key) {
    auto r = clht_get(iclht->ht, key);
    return true;
  }
  std::string hash_name() { return "CLHT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    clht_put(iclht, key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    // clht_put_replace(iclht, key, key);
    return false;
  }
  bool erase(size_t key, int tid) {
    clht_remove(iclht, key);
    return true;
  }
  void wait() {}
  void space_consumption() {}
};
#endif

#ifdef SOFTT
#include "third/SOFT/SOFTList.h"
constexpr size_t BUCKET_NUM = 16 * 1024 * 1024;
class hash_api {
 public:
   hash_api() { table = new SOFTList<size_t>[BUCKET_NUM];}
  ~hash_api() { printf("extra write count: %lu\n", soft_write_count.load()); }

  std::string hash_name() { return "SOFT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    SOFTList<size_t> &bucket = getBucket(key);
    bucket.insert(key, key, tid);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    SOFTList<size_t> &bucket = getBucket(key);
    bucket.remove(key);
    bucket.insert(key, key, tid);
    return true;
  }
  void thread_ini(int id) {
    init_alloc(id);
    init_volatileAlloc(id);
  }

  bool find(size_t key) {
    SOFTList<size_t> &bucket = getBucket(key);
    auto r = bucket.contains(key);
    return true;
  }

  bool erase(size_t key, int tid) {
    SOFTList<size_t> &bucket = getBucket(key);
    bucket.remove(key);
    return true;
  }
  void space_consumption() {
    size_t used_dram = 0, used_pmem = 0;
    for (size_t i = 0; i < BUCKET_NUM; i++)
    {
      auto [td, tp] = table[i].get_total_space();
      used_dram += td;
      used_pmem += tp;
    }
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
  }

  void recovery()
  {
    table = new SOFTList<size_t>[BUCKET_NUM];
    auto recover = [&](int tid)
    {
      auto curr = alloc[tid]->mem_chunks;
      for (; curr != nullptr; curr = curr->next)
      {
        PNode<size_t> *currChunk = static_cast<PNode<size_t> *>(curr->obj);
        uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(PNode<size_t>);
        for (uint64_t i = 0; i < numOfNodes; i++)
        {
          PNode<size_t> *currNode = currChunk + i;
          // if (currNode->key == 0) continue;
          if (!currNode->isValid() || currNode->isDeleted())
          {
            currNode->validStart = currNode->validEnd.load();
            // ssmem_free(alloc[tid], currNode, true);
          }
          else
          {
            auto k = h(currNode->key.load()) % BUCKET_NUM;
            table[k].quickInsert(currNode, true);
          }
        }
      }
    };
    std::vector<std::thread> t;
    for (size_t i = 0; i < SOFT_THREAD_NUM; i++)
    {
      t.push_back(std::thread(recover, i));
    }
    for (auto &v : t)
      v.join();
  }

  void wait() {}

 private:
  static size_t h(size_t k, size_t _len = 8,
                  size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
    auto _ptr = reinterpret_cast<void *>(&k);
    return std::_Hash_bytes(_ptr, _len, _seed);
  }
  SOFTList<size_t> &getBucket(uintptr_t k) { return table[h(k) % BUCKET_NUM]; }
  SOFTList<size_t> *table;
};
#endif

#ifdef VIPERT
#include <filesystem>
#define NONVAR 1
// #define VARVALUE 1
#include "third/viper/viper.hpp"

class hash_api {
#ifdef NONVAR
  using KEY_TYPE = uint64_t;
  using VALUE_TYPE = uint64_t;
#else
  using KEY_TYPE = std::string;
  using VALUE_TYPE = std::string;
#endif

public:
  std::unique_ptr<viper::Viper<KEY_TYPE, VALUE_TYPE>> viper_db;
  hash_api()
  {
    const size_t inital_size = pool_size;
    index_pool_name += "VIPER.data";
    const auto start = std::chrono::steady_clock::now();
    if (!std::filesystem::exists(index_pool_name))
      viper_db = viper::Viper<KEY_TYPE, VALUE_TYPE>::create(index_pool_name,
                                                            inital_size);
    else
    {
      viper_db = viper::Viper<KEY_TYPE, VALUE_TYPE>::open(index_pool_name);
    }
  }
  ~hash_api() {}
  std::string hash_name() { return "Viper"; };
  bool find(size_t key, viper::Viper<KEY_TYPE, VALUE_TYPE>::Client &c,
            void *v)
  {
#ifdef NONVAR
    auto r = c.get(key, (uint64_t*)v);
#else
    std::string new_key = std::to_string(key);
    auto r = c.get(new_key, (std::string*)v);
#endif
    return true;
  }

  bool insert(size_t key, size_t value_len, char *value,
              viper::Viper<KEY_TYPE, VALUE_TYPE>::Client &c)
  {
#ifdef NONVAR
    c.put(key, key);
#else
    auto new_key = std::to_string(key);
    std::string new_value;
    new_value.assign(value, value_len);
    c.put(new_key, new_value);
#endif
    return true;
  }
  bool update(size_t key, size_t value_len, char *value,
              viper::Viper<KEY_TYPE, VALUE_TYPE>::Client &c)
  {
#ifdef NONVAR
    auto update_fn = [&](uint64_t *value_addr)
    {
      *(value_addr) = key;
      pmem_persist(value_addr, sizeof(uint64_t));
    };
    c.update(key, update_fn);
#else
    std::string new_key = std::to_string(key);
    std::string new_value;
    new_value.assign(value, value_len);
    c.put(new_key, new_value);
#endif
    return true;
  }
  viper::Viper<KEY_TYPE, VALUE_TYPE>::Client get_client()
  {
    return viper_db->get_client();
  }

  bool erase(size_t key, viper::Viper<KEY_TYPE, VALUE_TYPE>::Client &c)
  {
#ifdef NONVAR
    c.remove(key);
#else
    std::string new_key = std::to_string(key);
    c.remove(new_key);
#endif
    return true;
  }
  void wait() {}
  void space_consumption()
  {
    auto [used_dram, used_pmem] = viper_db->space_consumption();
    printf("DRAM Usage: %lf, PMEM usage: %lf GB\n",
           used_dram / (1024.0 * 1024.0 * 1024.0),
           used_pmem / (1024.0 * 1024.0 * 1024.0));
  }
};
#endif
