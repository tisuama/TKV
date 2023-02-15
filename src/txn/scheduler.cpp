#include "txn/scheduler.h"
#include "proto/store.pb.h"

namespace TKV {

Action to_txn_action(pb::OpType op_type) {
    switch (op_type) {
        case pb::OP_PWRITE:
            return Action::Pwrite;
        case pb::OP_COMMIT:
            return Action::Commit;
        default:
            return Action::ActionNone;
    };
    return Action::ActionNone;
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
        int64_t region_id,
        int64_t term,
        pb::StoreReq*  req, 
        pb::StoreRes*  res, 
        google::protobuf::Closure* done) {

    auto txn_ctx = new TxnContext;
    txn_ctx->action = action;
    txn_ctx->region_id = region_id;
    txn_ctx->term = term;
    txn_ctx->req = req;
    txn_ctx->res = res;
    txn_ctx->done = done;
    
    if (txn_ctx->action == Pwrite) {
        auto pwrite_req = req->mutable_pwrite_req();
        auto pwriter = std::make_shared<Pwriter>(pwrite_req->start_version(), 
                                pwrite_req->lock_ttl(), 
                                pwrite_req->txn_size(), 
                                pwrite_req->min_commit_ts(), 
                                pwrite_req->max_commit_ts());

        for (auto& m : pwrite_req->mutable_mutations()) {
            pwriter->add_mutation(m);
        }
        for (auto& key: pwrite_req->mutable_secondaries()) {
            pwriter->add_secondary(key);   
        }
        pwriter->process_write(txn_ctx);
    } else if (txn_ctx->action == Commit) {
        // 
    }

}

void Scheduler::execute(TxnContext* txn_ctx) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
