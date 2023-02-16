#pragma once
#include "common/common.h"

namespace TKV {
struct LockRef {
    LockRef(): ref(0) {
    }

    int             ref;
    bthread::Mutex  mu;
};    

// TODO: RAII锁释放
class LockTable {
public:
    LockRef* lock_key(const std::string& key) {
        LockRef* find_lock = nullptr;
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            if (_table.find(key) == _table.end()) {
                find_lock = new LockRef;
                _table.emplace(key, find_lock);
            } else {
                find_lock = _table[key];
            }
        }
        // 阻塞等锁
        find_lock->mu.lock();
        find_lock->ref++;
        return find_lock;
    }

    LockRef* get(const std::string& key) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        LockRef* find_lock = nullptr;
        if (_table.find(key) != _table.end()) {
            find_lock = _table[key];
        }
        return find_lock;
    }

    void remove(const std::string& key) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table.find(key);
        it->second->ref--;
        it->second->mu.unlock();
        if (it->second->ref == 0) {
            _table.erase(it);
            // remove from table
            delete it->second;
        }
    }
    
private:
    bthread::Mutex _table_mutex;
    std::unordered_map<std::string, LockRef*> _table;
};

class ConcurrencyManager {
public:
    ConcurrencyManager(uint64_t latest_ts, int64_t region_id)
        : _max_ts(latest_ts)
        , _region_id(region_id)
    {}

    // Update first before use
    void update_max_ts(uint64_t new_ts) {
        if (new_ts != UINT64_MAX && new_ts > _max_ts.load()) {
            _max_ts.store(new_ts, std::memory_order_seq_cst);
        }
    }

    LockRef* lock_key(const std::string& key) {
        return _lock_table.lock_key(key);
    }

    std::vector<LockRef*> lock_keys(std::vector<std::string>& keys) {
        std::vector<LockRef*> result;
        sort(keys.begin(),  keys.end());
        for (auto& key: keys) {
            result.push_back(_lock_table.lock_key(key));
        }
        return result;
    }

private:
    int64_t                  _region_id;
    std::atomic<uint64_t>    _max_ts {0};
    LockTable                _lock_table;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
