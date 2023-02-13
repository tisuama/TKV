#pragma once
#include "txn/txn.h"

namespace TKV {

Action to_txn_action(pb::OpType op_type);    
    
class Scheduler {
public:
    Scheduler()
        : _stopped(false)
        , _latches(256)
    {}
    
    // TxnLock注意释放内存
    TxnLock* acquire_lock(uint64_t start_ts, std::vector<std::string>& keys);
    
    void release_lock(TxnLock* lock);
    
    void sched_command(Action action, 
            StoreRequest* req, StoreResponse* res, google::protobuf::Closure* done);

    void execute(TxnContext* txn_ctx);

private:

    bool        _stopped;
    Latches     _latches;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
