#pragma once
#include <cctype>

#include <bthread/bthread.h>
#include <butil/time.h>
#include <butil/endpoint.h>
#include <butil/fast_rand.h>
#include <butil/errno.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/execution_queue.h>


#include "common/log.h"

namespace TKV {
// Function define in common.cpp, extern here
extern int64_t parse_snapshot_index_from_path(const std::string& snapshot_path, bool use_dirname); 

#define RETURN_IF_NOT_INIT(init, response, log_id) \
    do {\
        if (!init) {\
            DB_WARNING("have not init, log_id: %lu", log_id);\
            response->set_errcode(pb::HAVE_NOT_INIT);\
            response->set_errmsg("have not init");\
            return ;\
        }\
    } while(0);

#define IF_DONE_SET_RESPONSE(done, errcode, err_msg) \
    do {\
        if (done && static_cast<MetaServerClosure*>(done)->response) {\
            auto meta_done = static_cast<MetaServerClosure*>(done);\
            meta_done->response->set_errcode(errcode);\
            meta_done->response->set_errmsg(err_msg);\
       }\
    }while (0);

#define ERROR_SET_RESPONSE(response, errcode, err_msg, op_type, log_id) \
    do {\
        DB_FATAL("request op_type: %d, %s, log_id: %lu", \
                op_type, err_msg, log_id);\
        if (response) { \
            response->set_errcode(errcode);\
            response->set_errmsg(err_msg);\
            response->set_op_type(op_type);\
        }\
    } while(0);

#define ERROR_SET_RESPONSE_FAST(response, errcode, err_msg, log_id) \
    do {\
        DB_FATAL("request failed errcode: %d, err_msg: %s, log_id: %lu", \
                 err_msg, log_id);\
        if (response) { \
            response->set_errcode(errcode);\
            response->set_errmsg(err_msg);\
        }\
    } while(0);

class ScopeGuard {
public:
    explicit ScopeGuard(std::function<void()> exit_func): 
       _exit_func(exit_func) {}
    ~ScopeGuard() {
        if (!_is_release) {
            _exit_func();
        }
    } 
    
    void release() {
        _is_release = true;
    }

private:
    std::function<void()> _exit_func;        
    bool _is_release {false};
    DISALLOW_COPY_AND_ASSIGN(ScopeGuard);
};

#define SCOPEGUARD_LINENAME_CAT(name, line) name##line
#define SCOPEGUARD_LINENAME(name, line) SCOPEGUARD_LINENAME_CAT(name, line) 
#define ON_SCOPED_EXIT(callback) ScopeGuard SCOPEGUARD_LINENAME(scoped_gurad, __LINE__)(callback)

class Bthread {
public:
    Bthread() {
    }
    explicit Bthread(const bthread_attr_t* attr) : _attr(attr) {
    }

    void run(const std::function<void()>& call) {
        std::function<void()>* _call = new std::function<void()>;
        *_call = call;
        int ret = bthread_start_background(&_tid, _attr, 
                [](void*p) -> void* { 
                    auto call = static_cast<std::function<void()>*>(p);
                    (*call)();
                    delete call;
                    return NULL;
                }, _call);
        if (ret != 0) {
            DB_FATAL("bthread_start_background fail");
        }
    }
    void run_urgent(const std::function<void()>& call) {
        std::function<void()>* _call = new std::function<void()>;
        *_call = call;
        int ret = bthread_start_urgent(&_tid, _attr, 
                [](void*p) -> void* { 
                    auto call = static_cast<std::function<void()>*>(p);
                    (*call)();
                    delete call;
                    return NULL;
                }, _call);
        if (ret != 0) {
            DB_FATAL("bthread_start_urgent fail");
        }
    }
    void join() {
        bthread_join(_tid, NULL);
    }
    bthread_t id() {
        return _tid;
    }

private:
    bthread_t _tid;
    const bthread_attr_t* _attr = NULL;
};
class BthreadCond {
public:
    BthreadCond(int count = 0) {
        bthread_cond_init(&_cond, NULL);
        bthread_mutex_init(&_mutex, NULL);
        _count = count;
    }
    ~BthreadCond() {
        bthread_mutex_destroy(&_mutex);
        bthread_cond_destroy(&_cond);
    }

    int count() const {
        return _count;
    }

    void increase() {
        bthread_mutex_lock(&_mutex);
        ++_count;
        bthread_mutex_unlock(&_mutex);
    }

    void decrease_signal() {
        bthread_mutex_lock(&_mutex);
        --_count;
        bthread_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
    }

    void decrease_broadcast() {
        bthread_mutex_lock(&_mutex);
        --_count;
        bthread_cond_broadcast(&_cond);
        bthread_mutex_unlock(&_mutex);
    }
    
    int wait(int cond = 0) {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count > cond) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    int increase_wait(int cond = 0) {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count + 1 > cond) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        ++_count; // ????????????while??????
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    int timed_wait(int64_t timeout_us, int cond = 0) {
        int ret = 0;
        timespec tm = butil::microseconds_from_now(timeout_us);
        bthread_mutex_lock(&_mutex);
        while (_count > cond) {
            ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }

    int increase_timed_wait(int64_t timeout_us, int cond = 0) {
        int ret = 0;
        timespec tm = butil::microseconds_from_now(timeout_us);
        bthread_mutex_lock(&_mutex);
        while (_count + 1 > cond) {
            ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break; 
            }
        }
        ++_count;
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    
private:
    int _count;
    bthread_cond_t _cond;
    bthread_mutex_t _mutex;
};

class TimeCost {
public:
    TimeCost(): _start(butil::gettimeofday_us()) {
    }
    ~TimeCost() {}
    
    void reset() {
        _start = butil::gettimeofday_us();
    }

    int64_t get_time() const {
        return butil::gettimeofday_us() - _start;
    }

private:
    int64_t _start;
};

// wrapper bthread::execution_queue functions for c++ style
class ExecutionQueue {
public:
    ExecutionQueue() {
        bthread::execution_queue_start(&_queue_id, nullptr, run_function, nullptr);
    }
    void run(const std::function<void()>& call) {
        bthread::execution_queue_execute(_queue_id, call);
    }
    void stop() {
        execution_queue_stop(_queue_id);
    }
    void join() {
        execution_queue_join(_queue_id);
    }
private:
    static int run_function(void* meta, bthread::TaskIterator<std::function<void()>>& iter) {
        if (iter.is_queue_stopped()) {
            return 0;
        }
        for (; iter; ++iter) {
            (*iter)();
        }
        return 0;
    }
    bthread::ExecutionQueueId<std::function<void()>> _queue_id = {0};
};    
template <typename KEY, typename VALUE, uint32_t MAP_COUNT = 23>
class ThreadSafeMap {
    static_assert( MAP_COUNT > 0, "Invalid MAP_COUNT parameters.");
public:
    ThreadSafeMap() {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            bthread_mutex_init(&_mutex[i], NULL);
        }
    }
    ~ThreadSafeMap() {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            bthread_mutex_destroy(&_mutex[i]);
        }
    }
    uint32_t count(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx].count(key);
    }
    uint32_t size() {
        uint32_t size = 0;
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            size += _map[i].size();
        }
        return size;
    }
    void set(const KEY& key, const VALUE& value) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        _map[idx][key] = value;
    }
    // ??????????????????????????????false???????????????init
    // init??????????????????0???????????????insert??????false
    bool insert_init_if_not_exist(const KEY& key, const std::function<int(VALUE& value)>& call) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        if (_map[idx].count(key) == 0) {
            if (call(_map[idx][key]) == 0) {
                return true;
            } else {
                _map[idx].erase(key);
                return false;
            }
        } else {
            return false;
        }
    }
    const VALUE get(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        if (_map[idx].count(key) == 0) {
            static VALUE tmp;
            return tmp;
        }
        return _map[idx][key];
    }
    const VALUE get_or_put(const KEY& key, const VALUE& value) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        if (_map[idx].count(key) == 0) {
            _map[idx][key] = value;
            return value;
        }
        return _map[idx][key];
    }
    VALUE& operator[](const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx][key];
    }

    bool exist(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx].count(key) > 0;
    }

    size_t erase(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx].erase(key);
    }
    // ?????????????????????????????????traverse?????????copy
    void traverse(const std::function<void(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                call(pair.second);
            }
        }
    }
    void traverse_with_key_value(const std::function<void(const KEY& key, VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                call(pair.first, pair.second);
            }
        }
    }
    void traverse_copy(const std::function<void(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            std::unordered_map<KEY, VALUE> tmp;
            {
                BAIDU_SCOPED_LOCK(_mutex[i]);
                tmp = _map[i];
            }
            for (auto& pair : tmp) {
                call(pair.second);
            }
        }
    }
    void clear() {
       for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            _map[i].clear();
        } 
    }
    // ???????????????true????????????init?????????false
    template<typename... Args>
    bool init_if_not_exist_else_update(const KEY& key, bool always_update, 
        const std::function<void(VALUE& value)>& call, Args&&... args) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter == _map[idx].end()) {
            _map[idx].insert(std::make_pair(key, VALUE(std::forward<Args>(args)...)));
            if (always_update) {
                call(_map[idx][key]);
            }
            return false;
        } else {
            //??????????????????????????????
            call(iter->second);
            return true;
        }
    }

    bool update(const KEY& key, const std::function<void(VALUE& value)>& call) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter != _map[idx].end()) {
            call(iter->second);
            return true;
        } else {
            return false;
        }
    }

    //????????????true??????????????????????????????false????????????????????????
    bool traverse_with_early_return(const std::function<bool(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                if (!call(pair.second)) {
                    return false;
                }
            }
        }
        return true;
    }

private:
    uint32_t map_idx(const KEY& key) {
        return std::hash<KEY>{}(key) % MAP_COUNT;
    }

private:
    std::unordered_map<KEY, VALUE> _map[MAP_COUNT];
    bthread_mutex_t _mutex[MAP_COUNT];
    DISALLOW_COPY_AND_ASSIGN(ThreadSafeMap);
};

inline std::string transfer_to_lower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), 
            [](unsigned char c) -> unsigned char { return std::tolower(c); });
    return str;
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
