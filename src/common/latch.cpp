#include "common/latch.h"
#include <xxhash.h>

namespace TKV {
int Latch::recycle(uint64_t current_ts) {
    std::vector<Node*> delete_node;
    Node node;
    node.next = queue;
    auto prev = &node;
    for (auto cur = prev->next; cur != nullptr; cur = cur->next) {
        if (TSO::tso_sub(current_ts, cur->max_commit_ts) >= LatchExpireTime && cur->value == nullptr) {
            count--;
            prev->next = cur->next;
            delete_node.push_back(cur);
        } else {
            prev = cur;
        }
    }
    queue = node.next;
    for (auto n : delete_node) {
        delete n;
    }
    return (int)delete_node.size();
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
    std::vector<int> target_slots;
    for (auto& key: keys) {
        int id = XXH32(key.data(), key.length(), 0) & (_slots.size() - 1);
        target_slots.push_back(id);
    }
    return target_slots;
}

bool Latches::acquire(TxnLock* lock) {
    // if (lock->is_stale) {
    //     return AcquireStale;
    // }
    int len = lock->required_slots.size();
    for (; lock->acquired_count < len; ) {
        auto s = acquire_slot(lock);
        if (!s) {
            return s;
        }
    }
    return true;
}  

bool Latches::acquire_slot(TxnLock* lock) {
    auto& key = lock->keys[lock->acquired_count];
    auto& slot_id = lock->required_slots[lock->acquired_count];
    auto& latch = _slots[slot_id];
    
    std::unique_lock<bthread_mutex_t> guard(latch.bmutex);
    
    // Try to recycle to limit memory
    if (latch.count >= LatchListCount) {
        latch.recycle(lock->start_ts);
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
        lock->acquired_count++;
        return true;
    }

    // commit_ts > start_ts情况下，当前事务无法commit
    // 原因在于对同一个key操作，commit时间戳更靠后，但是能被next_lock看到
    // if (find->max_commit_ts > lock->start_ts) {
    //     lock->is_stale = true;
    //     return AcquireStale;
    // }
    
    if (find->value == nullptr) {
        find->value = lock;
        lock->acquired_count++;
        return true;
    }

    // push the current transaction into waiting queue
    latch.waiting.push_back(lock);
    return false;
}


Node* Latches::find_node(Node* list, const std::string& key) {
    for (auto n = list; n != nullptr; n = n->next) {
        if (key == n->key) {
            return n;
        }
    }
    return nullptr;
}

std::vector<TxnLock*> Latches::release(TxnLock* lock) {
    std::vector<TxnLock*> weakup_list;
    for(; lock->acquired_count > 0; ) {
        auto next_lock = release_slot(lock);
        if (next_lock) {
            weakup_list.push_back(next_lock);
        }
    }
    return weakup_list;
}    

TxnLock* Latches::release_slot(TxnLock* lock) {
    auto& key = lock->keys[lock->acquired_count - 1];
    auto& slot_id = lock->required_slots[lock->acquired_count - 1];
    auto& latch = _slots[slot_id];
    lock->acquired_count--;

    std::unique_lock<bthread_mutex_t> guard(latch.bmutex);

    auto find = find_node(latch.queue, key);
    if (find->value != lock) {
        CHECK("something wrong" == 0);
    }

    find->max_commit_ts = std::max(find->max_commit_ts, lock->commit_ts);
    find->value = nullptr;

    if (latch.waiting.size() == 0) {
        return nullptr;
    }

    for (auto it = latch.waiting.begin(); it != latch.waiting.end(); it++) {
        auto wait = *it;
        if (wait->keys[wait->acquired_count] == key) {
            // 从waiting列表删除
            latch.waiting.erase(it);
            return wait;
        }
    }

    // weakup first one in the waiting queue
    // if (idx < (int)latch.waiting.size()) {
    //     auto next_lock = latch.waiting[idx];
    //     if (find->max_commit_ts > next_lock->start_ts) {
    //         find->value = next_lock;
    //         next_lock->acquired_count++;
    //         next_lock->is_stale = true;
    //     }
    //     return next_lock;
    // }
    return nullptr;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
