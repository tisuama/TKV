#pragma once
#include <unordered_map>
#include <string>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>
#include <bvar/status.h>


#include "common/common.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "engine/rocks_wrapper.h"
#include "meta/meta_server_interact.h"
#include "common/schema_factory.h"

namespace TKV {
DECLARE_int32(snapshot_load_num);
DECLARE_int32(raft_write_concurrency);
DECLARE_int32(service_write_concurrency);
DECLARE_int32(store_port);

class Store: public pb::StoreService {
public:
    virtual ~Store();
    static Store* get_instance() {
        static Store instance;
        return &instance;
    }    
    
    int init_before_listen(std::vector<std::int64_t>& init_region_ids);

    int init_after_listen(const std::vector<std::int64_t>& init_region_ids);

    virtual void init_region(::google::protobuf::RpcController* controller,
                         const ::TKV::pb::InitRegion* request,
                         ::TKV::pb::StoreRes* response,
                         ::google::protobuf::Closure* done) override;
    
    // construct heartbeat
    void construct_heart_beat_request(pb::StoreHBRequest& request);

    bool doing_snapshot_when_stop(int64_t region_id) {
        if (doing_snapshot_regions.find(region_id) != doing_snapshot_regions.end()) {
            return true;
        }
        return false;
    }
    std::set<int64_t>    doing_snapshot_regions;

private:
    Store()
        : _split_num(0)
        , _disk_total("disk_total", 0)
        , _disk_used("disk_used", 0)
        , _raft_total_cost("raft_total_cost", 0)
        , _heart_beat_count("heart_beat_count") {
    }  

    std::string              _address;
    std::string              _physical_room;    
    std::string              _resource_tag;
    RocksWrapper*            _rocksdb;
    
    // std::unordered_map<int64_t, SmartRegion> _region_map;
    MetaServerInteract*                       _meta_server_interact;

    Bthread                 _heart_beat_bth;
    TimeCost                _last_heart_time;
    Bthread                 _split_check_bth;
    // 全文索引的merge线程
    // Bthread                 _merge_bth;
    // 延迟删除region
    Bthread                 _delay_remove_data_bth;
    // 定时flush region meta信息，确保rocksdb的wal正常删除
    Bthread                 _flush_bth;
    // 外部控制定时触发snapshot
    Bthread                 _snapshot_bth;

    std::atomic<int32_t>    _split_num;
    bool                    _shutdown {false};

    bvar::Status<int64_t>   _disk_total;
    bvar::Status<int64_t>   _disk_used;
    
    bvar::LatencyRecorder   _raft_total_cost;
    bvar::LatencyRecorder   _select_time_cost;
    bvar::Adder<int64_t>    _heart_beat_count;

    // queue
    ExecutionQueue          _add_peer_queue;
    ExecutionQueue          _compact_queue;
    ExecutionQueue          _remove_region_queue;
    ExecutionQueue          _transfer_leader_queue;

    BthreadCond             _multi_thread_cond;
    bthread_mutex_t         _param_mutex;
    std::map<std::string, std::string> _param_map;
    
    SchemaFactory*           _factory;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
