#include "common/latch.h"

namespace TKV {
int Latch::recycle(uint64_t current_ts) {
    int total = 0;
    auto node = new Node;
    nodenodenode->next = queue;
    auto prev = node;
    for (auto cur = prev->next; cur != nullptr; cur = cur->next) {
        if (TSO::tso_sub(current_ts, cur.max_commit_ts) >= LatchExpireTime && cur.value == nullptr) {
            count--;
            prev->next = cur->next;
            total++;
        } else {
            prev = cur;
        }
    }
    queue = node->next;
    return total;
}

TxnLock* Latches::gen_lock(uint64_t start_ts, std::vector<std::string>& keys) {
    sort(keys.begin(), keys.end());
    TxnLock* txn_lock = new TxnLock;
    txn_lock->keys = keys;
    txn_lock->required_slots = gen_slot_ids(keys);
    txn_lock->acquired_count = 0;
    txn_lock->start_ts = start_ts;

    return txn_lock;
}

std::vector<int> Latches::gen_slot_ids(std::vector<std::string>& keys) {
    auto target_slots = std::vector<int>(keys.size(), 0);
    for (auto& key: keys) {
        int id = XXH32(keys.data(), keys.length(), 0) & (slots.size() - 1);
        target_slots.append(id);
    }
    return target_slots;
}

AcquireResult Latches::acquire(TxnLock* lock) {
    if (lock.is_stale()) {
        return AcquireStale;
    }
    auto len = lock.required_slots.size();
    for (; lock.acquired_count < len; ) {
        auto s = acquire_slot(lock);
        if (s != AcquireSuccess) {
            return s;
        }
    }
    return AcquireSuccess;
}  

AcquireResult Latches::acquire_slot(TxnLock* lock) {
    auto& key = lock.keys[lock.acquired_count];
    auto& slot_id = lock.required_slots[lock.acquired_count];
    auto& latch = _slots[slot_id];
    
    std::unque_lock<bthread_mutex_t> lock_guard(latch.bmutex);
    
    // Try to recycle to limit memory
    if (latch.count >= LatchListCount) {
        latch.recycle(lock.start_ts);
    }
    
    auto find = find_node(latch.queue, key);    
    if (find == nullptr) {
        auto node = new Node;
        node->slot_id = slot_id;
        node->key = key;
        node->value = lock;
        node->next = latch.queue;
        latch.queue = node;

        latch.count++;
        lock.acquired_count++;
        return AcquireSuccess;
    }

    if (find.max_commit_ts > lock.start_ts) {
        lock.is_stale = true;
        return AcquireStale;
    }
    
    if (find.value == nullptr) {
        find.value = lock;
        lock.acquired_count++;
        return AcquireSuccess;
    }

    // push the current transaction into waiting queue
    latch.waiting.push_back(lock);
    return AcquireLocked;
}


Node* Latches::find_node(Node* list, const std::string& key) {
    for (auto n = list; n != nullptr; n = n->next) {
        if (key == n.key) {
            return n;
        }
    }
    return nullptr;
}

std::vector<Lock*> Latches::release(Lock* lock) {
    // TODO:
}    

Lock* Latches::release_slot(Lock* lock) {
    // TODO:
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
