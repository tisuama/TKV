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

    int seq_id() {
        return _seq_id;
    }

    void set_seq_id(int seq_id) {
        if (seq_id < _seq_id) {
            DB_WARNING("txn_id: %lu seq_id: %d fallback, _seq_id: %ld", _txn_id, seq_id, _seq_id);
            return ;
        }
        _seq_id = seq_id;
    }
    
    struct TxnOptions {
        bool    dml_1pc {false};
        bool    in_fsm {false};
        int64_t lock_timeout {-1};
    };
    
    rocksdb::Transaction* get_txn() {
        return _txn;
    }

    void set_primary_region_id(int64_t region_id) {
        _primary_region_id = region_id;
    }

    void set_txn_timeout(int64_t timeout) {
        _txn_timeout = timeout;
    }

    void set_in_process(bool flag) {
        _in_process = flag;
    }

    bool is_finished() const {
        return _is_finished;
    }

    bool has_dml_executed() const {
        return _has_dml_executed;
    }

    bool need_write_rollback(pb::OpType op_type) {
        if (op_type == pb::OP_ROOLBACK && (_primary_region_id != -1)) {
            return true;
        }
        return false;
    }

    uint64_t rocksdb_txn_id() const {
        return _txn->GetID();
    }

    pb::StoreReq* get_raft_req() {
        return &_store_req;
    }

    void push_cmd_to_cache(int seq_id, pb::CachePlan plan_item);

    int begin(const Transaction::TxnOptions& txn_opt);

    int begin(const rocksdb::TransactionOptions& txn_opt);

    rocksdb::Status rollback();

    void rollback_current_request();

    rocksdb::Status prepare();

    void rollback_to_point(int seq_id);

    int set_save_point();
    
    void add_kvop_put(std::string& key, std::string& value, int64_t ttl_timestamp_us, bool is_primary_key);

    void add_kvop_delete(std::string& key, bool is_primary_key);

public:
    int64_t     num_increase_rows {0};
    int64_t     last_active_time {0};
    int64_t     begin_time {0};
    int         dml_num_affected_rows {0};
    // batch txn
    int64_t     batch_num_increase_rows {0};
    pb::ErrCode {pb::SUCCESS};

private:
    using CacheKVMap = std::map<int, pb::CachePlan>;

    int         _seq_id {0};
    int         _applied_seq_id {0};
    uint64_t    _txn_id {0};
    bool        _is_applying {false};
    bool        _is_prepared {false};
    bool        _is_finished {false};
    bool        _is_rolledback {false};
    // 正在处理该事务相关语句，事务内应该是串行，事务间并行
    std::atomic<bool>    _in_process {false};
    bool                 _write_begin_index {true};
    int64_t              _prepare_time_us{0};
    // put_cmd_to_cache时会设置成true
    bool                 _has_dml_executed {false};

    // region在执行事务前会通过txn->SetSavePoint()设置保存点，
    // 并且把当前的seq_id保存到栈save_point_seq中
    std::stack<int>      _save_point_seq;
    std::stack<uint64_t> _save_point_increase_rows;
    pb::StoreReq         _store_req;
    int64_t              _primary_region_id {-1};
    std::set<int>        _current_req_point_seq;

    // 执行语句失败后会将失败的seq_id添加到need_rollback_seq中
    std::set<int>        _need_rollback_seq;
    // store query cmd
    rocksdb::WriteOptions        _write_opt;
    rocksdb::TransactionOptions  _txn_opt;
    rocksdb::ColumnFamilyHandle* _data_cf {nullptr};
    rocksdb::ColumnFamilyHandle* _meta_cf {nullptr};
    const rocksdb::Snapshot*     _snapshot {nullptr};
    pb::RegionInfo*              _region_info {nullptr};
    RocksWrapper*                _txn_db {nullptr};
    rocksdb::Transaction*        _txn {nullptr};
    TransactionPool*             _pool {nullptr};

    bthread_mutex_t              _txn_mutex;
    bool                         _use_ttl {false};
    // 默认情况下设为seperate模式
    bool                         _is_seperate  {true};

    // TTL读取时间
    int64_t                      _read_ttl_timestamp_us {0};
    // TTL写入时间
    int64_t                      _write_ttl_timestamp_us {0};
    // 存量数据过期时间, online TTL 使用
    int64_t                      _online_ttl_us {0};
    int64_t                      _txn_timeout {0};
    // 实行的累计时间
    int64_t                      _txn_time_cost {0};
    // 缓存的命令
    bthread_mutex_t              _cache_kv_mutex;
    // seq_id => KvOp
    
    // Cache缓存OP_BEGIN/OP_INSERT/OP_UPDATE/OP_DELETE/OP_SELECT_FOR_UPDATE
    CacheKVMap                   _cache_kv_map;
};


typedef std::shared_ptr<Transaction> SmartTransaction;
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
