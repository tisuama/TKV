#pragma once
#include <rocksdb/db.h>
#include "proto/meta.pb.h"
#include "meta/common_state_machine.h"

namespace TKV {
class MetaStateMachine: public CommonStateMachine {
public:
    MetaStateMachine(const braft::PeerId& peerid) 
        : CommonStateMachine(0, "meta_raft", "/meta_server", peerid)
private:
    void save_snapshot(braft::Closure* done, 
                       rocksdb::Iterator* iter, 
                       braft::SnapshotWriter* writer);    

    int64_t _leader_start_stamp;
    Bthread _bth;
    bool    _health_check_start;

    bthread_mutex_t  _param_mutex;
    bool             _global_load_balance = true;
    std::map<std::string, bool> _resource_load_balance;

    bool            _global_migrate = true;
    std::map<std::string, bool> _resource_migrate;

    bool            _global_network_balance = true;
    std::map<std::string, bool> _resource_network_balance;

    bool    _unsafe_decision = false;
    int64_t _applied_index   = 0;

    bvar::LatencyReocrder _tkv_heart_beat;
    bvar::LatencyRecorder _store_heart_beat;
}
} // namespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
