#pragma once
#include <rocksdb/db.h>
#include "proto/meta.pb.h"
#include "meta/common_state_machine.h"

namespace TKV {

class MetaStateMachine: public CommonStateMachine {
public:
    MetaStateMachine(const braft::PeerId& peer_id) 
        : CommonStateMachine(0, "meta_raft", "/meta_server", peer_id)
        , _bth(&BTHREAD_ATTR_SMALL)
        , _health_check_start(false)
        , _tkv_heart_beat("tkv_heart_beat")
        , _store_heart_beat("store_heart_beat") {
        bthread_mutex_init(&_param_mutex, NULL);
    }  
    
    virtual ~MetaStateMachine() {
        bthread_mutex_destroy(&_param_mutex);
    }
    
    // raft 
    virtual void on_apply(braft::Iterator& iter) override;
    virtual void on_snapshot_save(braft::SnapshotWriter* writer,
                                  braft::Closure* done) override;
    virtual int on_snapshot_load(braft::SnapshotReader* reader) override;
    virtual void on_leader_start();
    virtual void on_leader_stop();
    
    int64_t applied_index() const {return _applied_index; }
    
    bool whether_can_decide();

    // service impl
    void store_heartbeat(::google::protobuf::RpcController* controller,
                         const ::TKV::pb::StoreHBRequest* request,
                         ::TKV::pb::StoreHBResponse* response,
                         ::google::protobuf::Closure* done);
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

    bvar::LatencyRecorder _tkv_heart_beat;
    bvar::LatencyRecorder _store_heart_beat;
};
} // namespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
