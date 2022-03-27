#pragma once
#include <braft/raft.h>
#include "common/common.h"
#include "proto/meta.pb.h"

namespace TKV {
class CommonStateMachine: public braft::StateMachine {
public:
    CommonStateMachine(int64_t dummy_region_id,
                       const std::string& identify,
                       const std::string& file_path,
                       const braft::PeerId& peer_id)
        : _node(identify, peer_id)
        , _is_leader(false)
        , _dummy_region_id(dummy_region_id)
        , _file_path(file_path)
        , _check_migrate(&BTHREAD_ATTR_SMALL) {}  
    virtual ~CommonStateMachine() {}
    virtual int init(const std::vector<braft::PeerId>& peers);

    virtual void raft_control(google::protobuf::RpcController* controller,
            const pb::RaftControlRequest* request,
            pb::RaftControlResponse* response,
            google::protobuf::Closure* done); 
    virtual void process(google::protobuf::RpcController* controller,
            const pb::MetaManagerRequest* request, 
            pb::MetaManagerResponse* response,
            google::protobuf::Closure* done);
    virtual void start_check_migrate();
    virtual void check_migrate();
    virtual void on_apply(braft::Iterator& iter) = 0;

    virtual void on_shutdown() {
        DB_WARNING("region_id: %ld raft is shutdown now", _dummy_region_id);
    }
    
    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) = 0;
    virtual int on_snapshot_load(braft::SnapshotReader* reader) = 0;
    virtual void on_leader_start();
    virtual void on_leader_start(int64_t term);
    virtual void on_leader_stop();
    virtual void on_leader_stop(const butil::Status& status);
    virtual void on_error(const butil::Error& e);
    virtual void on_configuration_committed(const braft::Configuration& conf);
    void start_check_bns();

    virtual butil::EndPoint get_leader() {
        return _node.leader_id().addr;
    }

    virtual void shutdown_raft() {
        _node.shutdown(NULL);
        DB_WARNING("raft node is shutdown");
        _node.join();
        DB_WARNING("raft node join completely");
    }

    virtual bool is_leader() const {
        return _is_leader;
    }

    bool have_data() {
        return _have_data;
    }

    void set_have_data(bool flag) {
        _have_data = flag;
    }

private:
    virtual int send_set_peer_request(bool remove_peer, const std::string& change_peer);

protected:
    braft::Node          _node;
    std::atomic<bool>    _is_leader;
    int64_t              _dummy_region_id;
    std::string          _file_path;

private:
    Bthread _check_migrate;
    bool    _check_state  {false};
    bool    _have_data    {false};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
