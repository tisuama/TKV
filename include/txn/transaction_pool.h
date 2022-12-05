#pragma once
#include "common/common.h"

#include <atomic>

namespace TKV {
class TransactionPool {
public:
    TransactionPool(): _num_prepared_txn(0), _txn_count(0) {}
    
    virtual ~TransactionPool() {}

    void close() {
        _txn_map.clear();
        _txn_count = 0;
    }

    SmartTransaction get_txn(uint64_t txn_id) {
        return _txn_map.get(txn_id);
    }

    int32_t num_prepared() {
        return _num_prepared_txn.count();
    }

    int32_t num_began() {
        return _txn_count.load();
    }

    bool use_ttl() const {
        return _use_ttl;
    }

    int64_t online_ttl_us() const {
        return _online_ttl_us;
    }
    
    int init(int64_t region_id, bool use_ttl, int64_t online_ttl_base_expire_time_us);

    int begin_txn(uint64_t txn_id, SmartTransaction& txn, int64_t primary_region_id, int64_t txn_timeout);

    void read_only_txn_process(int64_t region_id, SmartTransaction txn, pb::OpType op_type, bool optimize_1pc);
    
    void remove_txn(uint64_t txn_id, bool mark_finished);

private:
    int64_t     _region_id;
    int64_t     _latest_active_txn_ts {0};
    bool        _use_ttl {false};
    int64_t     _online_ttl_us {0};

    // txn_id => txn hander
    ThreadSafeMap<uint64_t, SmartTransaction>  _txn_map;
    // txn_id => affected_rows
    ThreadSafeMap<uint64_t, int>               _finished_txn_map; 

    TimeCost             _clean_finished_txn_cost;

    BthreadCond          _num_prepared_txn;
    std::atomic<int32_t> _txn_count;
    MetaWriter*          _meta_writer {nullptr};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
