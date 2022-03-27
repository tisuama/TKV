#pragma once
#include <bthread/bthread.h>
#include <butil/time.h>
#include <butil/endpoint.h>
#include <butil/fast_rand.h>
#include <butil/errno.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/execution_queue.h>


#include "common/log.h"

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
        ++_count; // 不能放在while前面
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
