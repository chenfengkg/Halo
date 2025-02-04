#ifndef _SOFT_LIST_H_
#define _SOFT_LIST_H_

#include <atomic>
#include <cassert>
#include <climits>
#include <string>

#include "VolatileNode.h"
#include "ssmem.h"
#include "utilities.h"
#define SOFT_THREAD_NUM 36
ssmem_allocator_t *alloc[SOFT_THREAD_NUM];
ssmem_allocator_t *volatileAlloc[SOFT_THREAD_NUM];
extern VMEM *vmp1;
#define UNLIKELY(x) __builtin_expect((x), 0)
#define LIKELY(x) __builtin_expect((x), 1)
#define compiler_fence std::atomic_thread_fence(std::memory_order_release)
#define MFENCE __sync_synchronize
typedef unsigned char uchar;
typedef softUtils::state state;

#define DRAM_INDEX
void init_alloc(int id) {
  auto r = vmem_malloc(vmp1, sizeof(ssmem_allocator_t));
  alloc[id] = (ssmem_allocator_t *)r;
  ssmem_alloc_init(alloc[id], SSMEM_DEFAULT_MEM_SIZE, id, 1);
}
void init_volatileAlloc(int id) {
  volatileAlloc[id] = (ssmem_allocator_t *)malloc(sizeof(ssmem_allocator_t));
  ssmem_alloc_init(volatileAlloc[id], SSMEM_DEFAULT_MEM_SIZE, id, 0);
}
template <class T>
class SOFTList {
 private:
  PNode<T> *allocNewPNode(int tid) {
    auto r = static_cast<PNode<T> *>(ssmem_alloc(alloc[tid], sizeof(PNode<T>), 1));
    return r;
  }

  Node<T> *allocNewVolatileNode(uintptr_t key, T value, PNode<T> *pptr,
                                bool pValidity, int tid) {
    Node<T> *n =
        static_cast<Node<T> *>(ssmem_alloc(volatileAlloc[tid], sizeof(Node<T>), 0));
    n->key = key;
    n->value = value;
    n->pptr = pptr;
    n->pValidity = pValidity;
    return n;
  }

 public:
  SOFTList() {
    // there is no need to save the sentinel nodes in the special areas
    head = new Node<T>(0, 0, nullptr, false);
    head->next.store(new Node<T>(ULLONG_MAX, 0, nullptr, false),
                     std::memory_order_release);
#ifdef PM_COUNT
    hash_api::performance.FENCE_count++;
#endif
  }

  std::pair<size_t, size_t> get_total_space()
  {
    size_t total_node = 1;
    Node<T> *curr = head->next.load();
    Node<T> *currRef = softUtils::getRef<Node<T>>(curr);
    while (currRef->key != ULLONG_MAX)
    {
      total_node++;
      curr = currRef->next.load();
      currRef = softUtils::getRef<Node<T>>(curr);
    }
    auto total_dram = total_node * sizeof(Node<T>);
    auto total_pmem = total_node * sizeof(PNode<T>);
    return {total_dram, total_pmem};
  }

 private:
  bool trim(Node<T> *prev, Node<T> *curr) {
    state prevState = softUtils::getState(curr);
    Node<T> *currRef = softUtils::getRef<Node<T>>(curr);
    Node<T> *succ = softUtils::getRef<Node<T>>(currRef->next.load());
    succ = softUtils::createRef<Node<T>>(succ, prevState);
    bool result = prev->next.compare_exchange_strong(curr, succ);
    // if (result) ssmem_free(alloc, currRef->pptr, true);
    return result;
  }

  // returns clean reference in pred, ref+state of pred in return and the state
  // of curr in the last arg
  Node<T> *find(uintptr_t key, Node<T> **predPtr, state *currStatePtr) {
    Node<T> *prev = head, *curr = prev->next.load(), *succ, *succRef;
    Node<T> *currRef = softUtils::getRef<Node<T>>(curr);
    state prevState = softUtils::getState(curr), cState;
    while (true) {
      succ = currRef->next.load();
      succRef = softUtils::getRef<Node<T>>(succ);
      cState = softUtils::getState(succ);
      if (LIKELY(cState != state::DELETED)) {
        if (UNLIKELY(currRef->key >= key)) break;
        prev = currRef;
        prevState = cState;
      } else {
        trim(prev, curr);
      }
      curr = softUtils::createRef<Node<T>>(succRef, prevState);
      currRef = succRef;
    }
    *predPtr = prev;
    *currStatePtr = cState;
    return curr;
  }

 public:
  bool insert(uintptr_t key, T value, int tid) {
    {
      // test PM insert performance
      // PNode<T> *newPNode = allocNewPNode();
      // newPNode->create(key, value, true);
      // return true;
    }
    Node<T> *pred, *currRef;
    state currState, predState;
  retry:
    while (true) {
      Node<T> *curr = find(key, &pred, &currState);
      currRef = softUtils::getRef<Node<T>>(curr);
      predState = softUtils::getState(curr);

      Node<T> *resultNode;
      bool result = false;

      if (currRef->key == key) {
        resultNode = currRef;
        if (currState != state::INTEND_TO_INSERT) return false;
      } else {
#ifdef DRAM_INDEX
        PNode<T> *newPNode = nullptr;
        bool pValid = true;
#else
        PNode<T> *newPNode = allocNewPNode(tid);
        bool pValid = newPNode->alloc();
#endif
        // soft_write_count++;
        Node<T> *newNode = allocNewVolatileNode(key, value, newPNode, pValid, tid);
        newNode->next.store(static_cast<Node<T> *>(softUtils::createRef(
                                currRef, state::INTEND_TO_INSERT)),
                            std::memory_order_relaxed);
        if (!pred->next.compare_exchange_strong(
                curr, static_cast<Node<T> *>(
                          softUtils::createRef(newNode, predState)))) {
          ssmem_free(volatileAlloc[tid], newNode, false);
#ifndef DRAM_INDEX
          ssmem_free(alloc[tid], newPNode, true);
#endif
          goto retry;
        }
        resultNode = newNode;
        result = true;
      }

#ifndef DRAM_INDEX
      resultNode->pptr->create(resultNode->key, resultNode->value,
                               resultNode->pValidity);
#endif
      while (softUtils::getState(resultNode->next.load()) ==
             state::INTEND_TO_INSERT)
        softUtils::stateCAS<Node<T>>(resultNode->next, state::INTEND_TO_INSERT,
                                     state::INSERTED);

      return result;
    }
  }

  bool remove(uintptr_t key) {
    bool casResult = false;
    Node<T> *pred, *curr, *currRef;
    state currState;
    curr = find(key, &pred, &currState);
    currRef = softUtils::getRef<Node<T>>(curr);
    softUtils::getState(curr);

    if (currRef->key != key) {
      return false;
    }

    if (currState == state::INTEND_TO_INSERT || currState == state::DELETED) {
      return false;
    }

    while (!casResult &&
           softUtils::getState(currRef->next.load()) == state::INSERTED)
      casResult = softUtils::stateCAS<Node<T>>(currRef->next, state::INSERTED,
                                               state::INTEND_TO_DELETE);
#ifndef DRAM_INDEX
    currRef->pptr->destroy(currRef->pValidity);
#endif

    while (softUtils::getState(currRef->next.load()) == state::INTEND_TO_DELETE)
      softUtils::stateCAS<Node<T>>(currRef->next, state::INTEND_TO_DELETE,
                                   state::DELETED);

    if (casResult) trim(pred, curr);
    return casResult;
  }

  T *contains(uintptr_t key) {
    Node<T> *curr = head->next.load();
    while (curr->key < key) {
      curr = softUtils::getRef<Node<T>>(curr->next.load());
    }
    state currState = softUtils::getState(curr->next.load());
    if ((curr->key == key) && ((currState == state::INSERTED) ||
                               (currState == state::INTEND_TO_DELETE)))
      return &curr->value;
    else
      return nullptr;
  }

  std::string myName() { return "SOFT List"; }

  void quickInsert(PNode<T> *newPNode, bool pValid) {
    uintptr_t key = newPNode->key;
    Node<T> *newNode = new Node<T>(key, newPNode->value, newPNode, pValid);

    Node<T> *pred = nullptr, *curr = nullptr, *succ = nullptr;
    Node<T> *currRef = nullptr, *succRef = nullptr;
    state predState, currState;
  retry:
    pred = head;
    curr = pred->next.load();
    currRef = (softUtils::getRef<Node<T>>(curr));
    while (true) {
      succ = currRef->next.load();
      currState = softUtils::getState(succ);
      succRef = (softUtils::getRef<Node<T>>(succ));
      // trimming
      while (currState == state::DELETED) {
        assert(false);
      }
      // continue searching
      if (currRef->key < key) {
        pred = currRef;
        curr = succ;
        currRef = (softUtils::getRef<Node<T>>(curr));
      }
      // found the same
      else if (currRef->key == key) {
        assert(false);
      } else {
        newNode->next.store(
            (softUtils::createRef<Node<T>>(currRef, state::INSERTED)),
            std::memory_order_relaxed);
        if (!pred->next.compare_exchange_strong(
                curr,
                (softUtils::createRef<Node<T>>(newNode, state::INSERTED))))
          goto retry;
        return;
      }
    }
  }
  void recovery(int tid) {
    auto curr = alloc[tid]->mem_chunks;
    for (; curr != nullptr; curr = curr->next) {
      PNode<T> *currChunk = static_cast<PNode<T> *>(curr->obj);
      uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(PNode<T>);
      for (uint64_t i = 0; i < numOfNodes; i++) {
        PNode<T> *currNode = currChunk + i;
        // if (currNode->key == 0) continue;
        if (!currNode->isValid() || currNode->isDeleted()) {
          currNode->validStart = currNode->validEnd.load();
          ssmem_free(alloc[tid], currNode, true);
        } else
          quickInsert(currNode, true);
        // std::cout << currNode->key << std::endl;
      }
    }
  }
  Node<T> *head;
};

#endif
