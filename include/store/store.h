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


#include "store/region.h"
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
class MetaWriter;
class Region;
typedef std::shared_ptr<Region> SmartRegion;
using DoubleBufRegion = butil::DoublyBufferedData<std::unordered_map<int64_t, SmartRegion>>;

class Store: public pb::StoreService {
public:
    virtual ~Store() {
        bthread_mutex_destroy(&_param_mutex);
    }

    static Store* get_instance() {
        static Store instance;
        return &instance;
    }    
    
    int init_before_listen(std::vector<std::int64_t>& init_region_ids);

    int init_after_listen(const std::vector<std::int64_t>& init_region_ids);

    // rpc function
    virtual void init_region(::google::protobuf::RpcController* controller,
                         const ::TKV::pb::InitRegion* request,
                         ::TKV::pb::StoreRes* response,
                         ::google::protobuf::Closure* done) override;
    virtual void query(::google::protobuf::RpcController* controller,
                        const ::TKV::pb::StoreReq* request, 
                        ::TKV::pb::StoreRes* response,
                        ::google::protobuf::Closure* done) override;
    
    
    // construct heartbeat
    void construct_heart_beat_request(pb::StoreHBRequest& request);
    
    bool doing_snapshot_when_stop(int64_t region_id) {
        if (_doing_snapshot_regions.find(region_id) != _doing_snapshot_regions.end()) {
            return true;
        }
        return false;
    }

    SmartRegion get_region(int64_t region_id) {
        DoubleBufRegion::ScopedPtr ptr;
        if (_region_mapping.Read(&ptr) == 0) {
            auto iter = ptr->find(region_id);
            if (iter != ptr->end()) {
                return iter->second;
            }
        }
        return SmartRegion();
    }
    
    void set_region(SmartRegion& region) {
        if (region == nullptr) {
            return ;
        }
        auto call = [](std::unordered_map<int64_t, SmartRegion>& mp, const SmartRegion& region) {
           mp[region->get_region_id()] = region; 
           return 1;
        };
        _region_mapping.Modify(call, region);
    }
    
    void erase_region(int64_t region_id) {
        auto call = [](std::unordered_map<int64_t, SmartRegion>& mp, const int64_t region_id) {
            mp.erase(region_id);
            return 1;
        };
        _region_mapping.Modify(call, region_id);//region_id
    }

    // traverse region
    void traverse_region_map(const std::function<void(const SmartRegion& region)>& call) {
        DoubleBufRegion::ScopedPtr ptr;
        // Read -1: Failed 0: success
        if (_region_mapping.Read(&ptr) == 0) {
            for (auto it : *ptr) {
                call(it.second);
            }
        }
    }
    
    void traverse_copy_region_map(const std::function<void(const SmartRegion& region)>& call) {
        std::unordered_map<int64_t, SmartRegion> copy_region_mapping;
        {
            DoubleBufRegion::ScopedPtr ptr;
            if (_region_mapping.Read(&ptr) == 0) {
                copy_region_mapping = *ptr;
            }
        }
        for (auto it : copy_region_mapping) {
            call(it.second);
        }
        
    }
    
    std::string address() const {
        return _address;
    }
    
    ExecutionQueue& compact_queue() {
        return _compact_queue;
    }
    
    bool is_shutdown() const {
        return _shutdown;
    }
    
    void shutdown_raft() {
        _shutdown = true;
        traverse_copy_region_map([](const SmartRegion& region) {
            region->shutdown();
        });
        traverse_copy_region_map([](const SmartRegion& region) {
            region->join();    
        });
        DB_WARNING("Store shutdown, All region shutdown");
    }
    
    void close() {
        _add_peer_queue.stop();
        _remove_region_queue.stop();
        _compact_queue.stop();
        _transfer_leader_queue.stop();
        _shutdown = true;
        _heart_beat_bth.join();
        _add_peer_queue.join();
        _remove_region_queue.join();
        _compact_queue.join();
        _transfer_leader_queue.join();
        _split_check_bth.join();
        _delay_remove_data_bth.join();
        _flush_bth.join();
        _snapshot_bth.join();
        _multi_thread_cond.wait();
        // close rocksdb
        _rocksdb->close();
    }

    void set_can_add_peer_for_region(int64_t region_id) {
        SmartRegion region = get_region(region_id);
        if (region == nullptr) {
            DB_FATAL("region_id: %ld not exist", region_id);
            return ;
        }
        region->set_can_add_peer();
    }

    void reset_region_status(int64_t region_id) {
        SmartRegion region = get_region(region_id);
        if (region == nullptr) {
            DB_FATAL("region_id: %ld not exist", region_id);
            return ;
        }
        region->reset_region_status();
    }

    void update_schema_info(const pb::SchemaInfo& table, std::map<int64_t, int64_t>* reverser_index_map);
    int drop_region_from_store(int64_t drop_region_id, bool need_delay_drop);
    
    // Bthread fn
    void heart_beat_thread();
    void send_heart_beat();
    void process_heart_beat_response(const pb::StoreHBResponse& response);

private:
    void check_region_legal_complete(int64_t region_id);

private:
    Store()
        : _split_num(0)
        , _disk_total("disk_total", 0)
        , _disk_used("disk_used", 0)
        , _raft_total_cost("raft_total_cost", 0)
        , _heart_beat_count("heart_beat_count") {
        bthread_mutex_init(&_param_mutex, NULL);
    }  

    std::string              _address;
    std::string              _physical_room;    
    std::string              _resource_tag;
    RocksWrapper*            _rocksdb;
    
    DoubleBufRegion          _region_mapping;
    
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
    std::set<int64_t>        _doing_snapshot_regions;
    
    MetaWriter*             _meta_writer {nullptr};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
