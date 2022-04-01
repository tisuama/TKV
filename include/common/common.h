#pragma once
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
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
