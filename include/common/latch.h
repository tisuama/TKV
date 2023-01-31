#pragma once

#include "common/common.h"

namespace TKV {
constexpr int LatchListCount  = 5; 
constexpr int LatchExpireTime = 120; // 120s

struct TxnLock {
    std::vector<std::string>    keys;
    std::vector<int>            required_slots;
    int                         acquired_count;
    uint64_t                    start_ts;
    uint64_t                    commit_ts;
    // 当前start_ts过期
    bool                        is_stale;

    bool is_stale() const {
        return is_stale;
    }

    bool is_locked() const {
        return !is_stale && acquired_count != required_slots.size(); 
    }
};

struct Node {
    int         slot_id;
    std::string key;
    uint64_t    max_commit_ts;
    TxnLock*    value;
    Node*       next;
};

// Latches  | latch1 | latch2 | latch3 |
//              ||
//              \/
//             node1
//              ||
//              \/
//             node2 

struct Latch {
    Latch() 
        : queue(NULL)
        , count(0)
    {
        bthread_mutex_init(&bmutex, NULL);
    }
    
    ~Latch() {
        bthread_mutex_destroy(&bmutex);
    }

    Node*   queue;      // 当前Latch下第一个节点
    int     count;      // 当前Latch下Lock节点数量
    std::vector<TxnLock> waiting; // 当前Latch下加锁失败等待的数量
    bthread_mutex_t      bmutex;
};

enum LatcheResult {
    AcquireSuccess = 0,
    AcquireLocked,
    AcquireStale
};

class Latches {
public:
    Latches(int size)
        : slots.resize(size)
    {}

    TxnLock* gen_lock(uint64_t start_ts, std::vector<std::string>& keys);
    
    AcquireResult acquire(TxnLock* lock);    

    AcquireResult acquire_slot(TxnLock* lock);

    // release all latches owned by the 'lock'
    std::vector<Lock*> release(Lock* lock);    
    
private:
    std::vector<int> gen_slot_ids(std::vector<std::string>& keys);

    int recycle(uint64_t current_ts);
    
    Node* find_node(Node* list, const std::string& key);

    Lock* release_slot(Lock* lock);

    std::vector<Latch> _slots;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
