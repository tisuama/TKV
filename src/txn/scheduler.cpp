#include "txn/scheduler.h"

namespace TKV {

Action to_txn_action(pb::OpType op_type) {
    switch op_type {
        case pb::OP_PWRITE:
            return Action::Pwrite;
        case pb::OP_COMMIT:
            return Action::Commit;
        default:
            return Action::AcionNone;
    };
    return Action::AcionNone;
}    

TxnLock* Scheduler::acquire_lock(uint64_t start_ts, std::vector<std::string>& keys) {
    auto txn_lock = _latches.gen_lock(start_ts, keys);
    if (_latches.acquire(txn_lock) == false) {
        txn_lock->cond.increase_wait();
    }
    if (txn_lock->is_locked()) {
        CHECK("TxnLock locked failed" == 0);
    }
    return txn_lock;
}

void Scheduler::release_lock(TxnLock* lock) {
    auto wakeup_list = _latches.release(lock);
    
    // wake_fn
    if (wakeup_list.size() > 0) {
        auto wake_fn = [this, wakeup_list]() {
            for (auto lock: wakeup_list) {
                if (_latches.acquire(lock)) {
                    lock->cond.decrease_signal();
                }
            }
        };
        Bthread bth;
        bth.run(wake_fn);
    }
}

void Scheduler::sched_command(Action action, 
        StoreRequest* req, StoreResponse* res, google::protobuf::Closure* done) {
    auto txn_ctx = new TxnContext;
    auto txn = new Txn;
    txn_ctx->action = to_txn_action(req->op_type());
    if (txn_ctx->action == Pwrite) {
        auto pwrite_req = req->mutable_pwrite_req();
        txn_ctx->start_ts = pwrite_req->start_ts();
        txn_ctx->primary =  pwrite_req->primary_lock;
        for (auto& key: pwrite_req->secondaries()) {
            txn_ctx->keys.push_back(key);
        }
        txn_ctx->lock_ttl = pwrite_req->lock_ttl();
    }
}

void Scheduler::execute(TxnContext* txn_ctx) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
