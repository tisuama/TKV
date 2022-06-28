#pragma once
#include <atomic>
#include <mutex>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <braft/storage.h>
#include <braft/file_system_adaptor.h>

#include "common/common.h"
#include "common/schema_factory.h"
#include "common/table_key.h"
#include "common/mut_table_key.h"
#include "engine/rocks_wrapper.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "store/meta_writer.h"

namespace TKV {
struct RegionResource {
    pb::RegionInfo region_info;
};

class Region: public braft::StateMachine, public std::enable_shared_from_this<Region> {
friend class RegionControl;
public:
    virtual ~Region() {}
   
    Region(RocksWrapper* rocksdb, 
           SchemaFactory* factory,
           const std::string& address,
           const braft::GroupId& groupid,
           const braft::PeerId& peerid,
           const pb::RegionInfo& region_info,
           const int64_t region_id,
           bool is_learner = false)
        : _rocksdb(rocksdb)
        , _factory(factory)
        , _address(address)
        , _region_info(region_info)
        , _region_id(region_id)
        , _node(groupid, peerid)
        , _is_leader(false)
        , _shutdown(false)
        , _num_table_lines(0)
        , _num_delete_lines(0)
        // , _snapshot_adaptor(new braft::FileSystemAdaptor(region_id)
    {}

    void construct_heart_beat_request(pb::StoreHBRequest& request, bool need_peer_balance);
    
    butil::EndPoint get_leader() {
        if (is_learner()) {
            butil::EndPoint leader;
            butil::str2endpoint(region_info().leader().c_str(), &leader);
            return leader;
        }
        return _node.leader_id().addr;
    }
    
    pb::RegionInfo& region_info() {
        return _region_info;
    }
    
    int64_t get_table_id() {
        return _table_id;
    }
    
    int64_t get_region_id() const {
        return _region_id;
    }
    
    void set_restart(bool restart) {
        _restart = restart;
    }
    
    void copy_region(pb::RegionInfo* region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        region_info->CopyFrom(_region_info);
    }
    
    bool is_learner() const {
        return _is_learner;
    }
    
    bool is_leader() const {
        return _is_leader.load();
    }
    
    bool is_merged() {
        std::lock_guard<std::mutex> lock(_region_lock);
        if (!_region_info.start_key().empty()) {
            return _region_info.start_key() == _region_info.end_key();
        }
        return false;
    }

    bool removed() const {
        return _removed;
    }

    bool can_add_peer() const {
        return _region_info.can_add_peer();
    }

    // public
    void compact_data_in_queue();
    int init(bool new_region, int32_t snapshot_times);

    // override virtual functions from braft::StateMachine
    virtual void on_apply(braft::Iterator& iter) override; 

private:
    RocksWrapper*           _rocksdb;
    SchemaFactory*          _factory;
    rocksdb::ColumnFamilyHandle* _data_cf;
    rocksdb::ColumnFamilyHandle* _meta_cf;
    std::string             _address;

    // region meta info
    pb::RegionInfo          _region_info;
    std::mutex              _region_lock;

    // split region info
    std::vector<pb::RegionInfo> _new_region_infos;
    size_t                  _snapshot_data_size = 0;
    size_t                  _snapshot_meta_size = 0;
    pb::RegionInfo          _new_region_info;
    int64_t                 _region_id = 0;
    int64_t                 _version = 0;
    int64_t                 _table_id = 0;

    // merge 
    pb::RegionInfo          _merge_region_info;

    // BthreadCond
    BthreadCond             _disable_write_cond;
    BthreadCond             _real_writing_cond;
    
    // Legal 
    std::mutex              _legal_mutex;
    bool                    _legal_region = true;
    
    // restart
    bool                    _restart = false;

    // 计算存储分离开关
    bool                    _storage_compute_separate = false;

    // Raft
    braft::Node             _node;
    std::atomic<bool>       _is_leader;
    
    int64_t                 _braft_apply_index = 0;
    int64_t                 _applied_index = 0;
    int64_t                 _data_index = 0;
    int64_t                 _expect_term = -1; 
    
    bool                    _report_peer_info = false;
    std::atomic<bool>       _shutdown;
    bool                    _init_success = false; 
    bool                    _need_decrease = false;
    bool                    _can_heartbeat = false;

    // table line
    std::atomic<int64_t>    _num_table_lines;
    std::atomic<int64_t>    _num_delete_lines;
    
    bool                    _removed = false;
    std::string             _rocksdb_start;
    std::string             _rocksdb_end;
    pb::PeerStatus          _region_status = pb::STATUS_NORMAL;


    // learner
    bool                    _is_learner = false;
    bool                    _learner_ready_for_read = false;
    TimeCost                _learner_time;
    
    MetaWriter*              _meta_writer = nullptr;
    std::shared_ptr<RegionResource> _resource;
    // TODO: rocksdbfilesystemadaptor
    scoped_refptr<braft::FileSystemAdaptor> _snapshot_adaptor = nullptr;

};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
