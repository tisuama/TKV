#pragma once

namespace TKV {
struct LockMap {
    LockMap(): _ref(0) {
        bthread_mutex_init(&_mutex, NULL);
    }
    ~LockMap() {
        bthread_mutex_destroy(_mutex);
    }

    int             ref;
    bthread_mutex_t mu;
};    

// TODO: RAII锁释放
class LockTable {
public:
    LockMap* lock_key(const std::string& key) {
        LockMap* find_lock = nullptr;
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            if (_table.find(key) == _table.end()) {
                find_lock = new LockMap;
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

    LockMap* get(const std::string& key) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        LockMap* find_lock = nullptr;
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
            _table.remove(it);
            // remove from table
            delete it->second;
        }
    }
    
private:
    bthread::Mutex _table_mutex;
    std::unordered_map<std::string, LockMap*> _table;
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

    LockMap* lock_key(const std::string& key) {
        return _lock_table.lock_key(key);
    }

    std::vector<LockMap*> lock_keys(std::vector<std::string>& keys) {
        std::vector<LockMap*> result;
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
