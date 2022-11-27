#include "txn/transaction_pool.h"

namespace TKV {
int TransactionPool::init(int64_t region_id, bool use_ttl, int64_t online_ttl_base_expire_time_us) {
	_region_id = region_id;
	_use_ttl = use_ttl;
	_oneline_ttl_base_expire_time_us = online_ttl_base_expire_time_us;
	_meta_writer = MetaWriter::get_instance();
	return 0;
}

int TransactionPool::begin_txn(uint64_t txn_id, SmartTransaction& txn, int64_t primary_region_id, int64_t txn_timeout) {
    auto fn = [this, txn_id, primary_region_id, txn_timeout] (SmartTransaction& txn) {
        txn = SmartTransaction(new (std::nothrow)Transaction(txn_id, this));
        if (txn == nullptr) {
            DB_FATAL("region_id: %ld new txn failed, txn_id: %lu", _region_id, txn_id);
            return -1;
        }
        int ret = txn->begin(Transaction::TxnOptions());
        if (ret) {
            DB_FATAL("regioon_id: %ld begin txn failed, txn_id: %lu", _region_id, txn_id);
            txn.reset();
            return -1;
        }
        /* txn_name: region_id_txn_id */
        std::string name = std::to_string(_region_id) + "_" + std::string(txn_id);
        auto s = txn->get_txn()->SetName(txn_name);
        if (!s.ok()) {
            DB_FATAL("region_id: %ld set txn name failed, txn_id: %lu", _region_id, txn_id);
            return -1;
        }
        if (primary_region_id > 0) {
            txn->set_primary_region_id(primary_region_id);
        }
        if (txn_timeout > 0) {
            txn->set_txn_timeout(txn_timeout);
        }
        txn->set_in_process(true);
        _txn_count++;
        return 0;
    };
    if (!_txn_map.insert_init_if_not_exist(txn_id, fn)) {
        DB_FATAL("region_id: %ld, txn_id: %ld has already exist", _region_id, txn_id);
        return -1;
    }
    txn = _txn_map.get(txn_id);
    _latest_active_txn_ts = butil::gettimeofday_us();
    return 0;
}

void TransactionPool::read_only_txn_process(int64_t region_id, SmartTransaction txn, pb::OpType op_type, bool optimize_1pc) {
    uint64_t txn_id = txn->txn_id();
    switch(op_type) {
        case pb::OP_PREPARE: {
            if (optimize_1pc) {
                txn->rollback();
                remove_txn(txn_id, true);
            } else {
                txn->prepare();
            }
            break;
        }
        case pb::ROLLBACK: 
            txn->rollback();
            remove_txn(txn_id, true);
            break;
        case pb::COMMIT:
            txn->rollback();
            remove_txn(txn_id, true);
            break;
        default:
            break;
    }
    DB_WARNING("region_id: %ld txn_id: %lu, optimize_1pc: %d, op_type: %s",
            _region_id, txn_id, optimize_1pc, pb::OpType_Name(op_type).c_str());
}

void TransactionPool::remove_txn(uint64_t txn_id, bool mark_finished) {
    auto txn = _txn_map.get(txn_id);
    if (txn == nullptr) {
        return ;
    }
    if (mark_finished) {
        _finished_txn_map[txn_id] = txn->dml_num_affected_rows;
    }
    auto size = _txn_map.size();
    if (size > 0) {
        _txn_count--;
    }
    _latest_active_txn_ts = butil::gettimeofday_us();
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
