#pragma once 
#include "common/common.h"
#include <braft/raft.h>

namespace TKV {
class SyncClosure: public braft::Closure {
public:
    void wait() {
        _cond.increase_wait();
    }
    
    void Run() {
        _cond.decrease_signal();
    }
private:
    BthreadCond _cond;
};    
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
