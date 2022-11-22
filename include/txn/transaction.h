#pragma once
#include "common/common.h"
#include "engine/rocks_wrapper.h"
#include "proto/store.pb.h"
#include <atomic>


namespace TKV {
DECLARE_bool(disable_wal);

class TransactionPool;
class Transaction {
public:
    Transaction(uint64_t txn_id, TransactionPool* pool)
        : _txn_id(txn_id)
        , _pool(pool)
    {
        _write_opt.disableWAL = FLAGS_disable_wal;
        bthread_mutex_init(&_txn_mutex, nullptr);
    }

private:
    int         _seq_id {0};
    int         _applied_seq_id {0};
    uint64_t    _txn_id {0};
    bool        _is_applying {false};
    bool        _is_prepared {false};
    bool        _is_finished {false};
    bool        _is_rolledback {false};
    std::atomic<bool>     _in_process {false};
    bool                 _write_begin_index {true};
    int64_t              _prepare_time_us{0};
    std::stack<int>      _save_point_seq;
    std::stack<uint64_t> _save_point_increase_rows;
    pb::StoreReq         _store_req;
    int64_t              _primary_region_id {-1};
    std::set<int>        _current_req_point_seq;
    std::set<int>        _need_rollback_seq;
    // store query cmd
    rocksdb::WriteOptions       _write_opt;
    rocksdb::TransactionOptions _txn_opt;
    const rocksdb::Snapshot*    _snapshot {nullptr};
    pb::RegionInfo*             _region_info {nullptr};
    TransactionPool*            _pool {nullptr};

    bthread_mutex_t             _txn_mutex;
    bool                        _use_ttl {false};
    bool                        _is_seperate  {false};
    int64_t                     _read_ttl_timestamp_us {0};
    int64_t                     _write_ttl_timestamp_us {0};
    int64_t                     _online_ttl_base_expire_time_us {0};
    int64_t                     _txn_timeout {0};
    int64_t                     _txn_time_cost {0};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
