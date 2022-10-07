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
#include "store/region_control.h"
#include "raft/rocksdb_file_system_adaptor.h"
#include "common/concurrency.h"

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
        , _region_control(this, region_id)  
        , _snapshot_adaptor(new RocksdbFileSystemAdaptor(region_id))
    {
        _region_control.set_status(_region_info.status());
        _version = _region_info.version();
        // not global index => _region_info.main_table_id() = _region_info.table_id()
        _table_id = _region_info.table_id();
    }

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
        BAIDU_SCOPED_LOCK(_region_lock);
        region_info->CopyFrom(_region_info);
    }
    
    bool is_learner() const {
        return _is_learner;
    }
    
    bool is_leader() const {
        return _is_leader.load();
    }
    
    bool is_merged() {
        BAIDU_SCOPED_LOCK(_region_lock);
        if (!_region_info.start_key().empty()) {
            return _region_info.start_key() == _region_info.end_key();
        }
        return false;
    }

    void set_removed(bool removed) {
        _removed = removed;
    }
    
    bool removed() const {
        return _removed;
    }

    bool can_add_peer() const {
        return _region_info.can_add_peer();
    }
    
    int64_t get_version() const {
        return _version;
    }

    void wait_async_apply_log_queue_empty() {
        BthreadCond cond;
        cond.increase();
        _async_apply_log_queue.run([&cond] {
            cond.decrease_signal();
        });
        cond.wait();
    }

    int64_t get_log_index() const {
        return _applied_index;
    }

    int64_t get_data_index() const {
        return _data_index;
    }

    void set_snapshot_meta_size(size_t sz) {
        _snapshot_meta_size = sz;
    } 
    
    void set_snapshot_data_size(size_t sz) {
        _snapshot_data_size = sz;
    }
    
    int64_t get_timecost() const {
        return _time_cost.get_time();
    }
    
    void reset_timecost() {
        return _time_cost.reset();
    }

    void get_node_status(braft::NodeStatus* status) {
        _node.get_status(status);
    }

    bool is_addpeer() const {
        return _region_info.can_add_peer();
    }

    pb::PeerStatus region_status() const {
        return _region_status;
    }

    uint64_t snapshot_data_size() const {
        return _snapshot_data_size;
    }
    
    uint64_t snapshot_meta_size() const {
        return _snapshot_meta_size;
    }
    
    uint64_t snapshot_index() const {
        return _snapshot_index;
    }

    bool compare_and_set_legal() {
        BAIDU_SCOPED_LOCK(_legal_mutex);
        if (_legal_region) {
            return true;
        }
        return false;
    }
    
    bool compare_and_set_illegal() {
        BAIDU_SCOPED_LOCK(_legal_mutex);
        BAIDU_SCOPED_LOCK(_region_lock);
        if (_region_info.version() <= 0) {
            _legal_region = false;
            return true;
        }
        return false;
    }

    void join() {
        _real_writing_cond.wait();
        _disable_write_cond.wait(); 
        DB_WARNING("region_id: %ld raft node join finish", _region_id);
    }

    void shutdown() {
        if (get_version() == 0) {
            wait_async_apply_log_queue_empty();
        }
        if (_need_decrease) {
            _need_decrease = false;
            Concurrency::get_instance()->receive_add_peer_concurrency.decrease_broadcast();
        }
        bool expect_status = false;
        if (_shutdown.compare_exchange_strong(expect_status, true)) {
            _init_success = false;
            DB_WARNING("region_id: %ld raft node is shutdown", _region_id);
        }
    }
    
    void set_can_add_peer() {
        if (!_region_info.can_add_peer()) {
            pb::RegionInfo region_pb;
            copy_region(&region_pb);
            region_pb.set_can_add_peer(true);
            if (_meta_writer->update_region_info(region_pb) != 0) {
                DB_FATAL("region_id: %ld set can add peer fail", _region_id);
            } else {
                DB_WARNING("region_id: %ld set can add peer success", _region_id);
            }
            _region_info.set_can_add_peer(true);
        }
    }
    
    void reset_region_status() {
        _region_control.reset_region_status(); 
    }

    bool learner_ready_for_read() const {
        return _learner_ready_for_read;
    }
    
    void add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue) {
        _region_control.add_peer(add_peer, region, queue);
    }

    // public
    void compact_data_in_queue();
    int init(bool new_region, int32_t snapshot_times);
    void reset_snapshot_status();
    bool check_region_legal_complete();

    // override virtual functions from braft::StateMachine
    virtual void on_apply(braft::Iterator& iter) override; 
    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
    virtual int  on_snapshot_load(braft::SnapshotReader* reader) override;
    virtual void on_shutdown();
    virtual void on_leader_start(int64_t term);
    virtual void on_leader_stop();
    virtual void on_leader_stop(const butil::Status& status);

    // rpc function called by Store
    void query(::google::protobuf::RpcController* controller,
               const ::TKV::pb::StoreReq* request, 
               ::TKV::pb::StoreRes* response,
               ::google::protobuf::Closure* done);
private:
    void on_snapshot_load_for_restart(braft::SnapshotReader* reader, 
            std::map<int64_t, std::string>& prepared_log_entrys);

    int ingest_snapshot_sst(const std::string& dir);

    int check_learner_snapshot();

    int check_follower_snapshot(const std::string& peer);

    void set_region_with_update_range(const pb::RegionInfo& region_info);

    bool valid_version(const pb::StoreReq* request, pb::StoreRes* response);
    
    void apply(const pb::StoreReq* request, pb::StoreRes* response, 
            brpc::Controller* cntl, google::protobuf::Closure* done); 

    void do_apply(int64_t term, int64_t index, const pb::StoreReq& request, braft::Closure* done);

    // Leader切换时确保事务状态一致，提交OP_CLEAR_APPLYING_TXN指令清理不一致事务
    void apply_clear_transaction_log();

    void leader_start(int64_t term);

private:
    RocksWrapper*           _rocksdb;
    SchemaFactory*          _factory;
    rocksdb::ColumnFamilyHandle* _data_cf;
    rocksdb::ColumnFamilyHandle* _meta_cf;
    std::string             _address;

    // region meta info
    pb::RegionInfo          _region_info;
    bthread::Mutex          _region_lock;

    // split region info
    std::vector<pb::RegionInfo> _new_region_infos;
    size_t                  _snapshot_data_size = 0;
    size_t                  _snapshot_meta_size = 0;
    pb::RegionInfo          _new_region_info;
    int64_t                 _region_id = 0;
    // 1) table_manager发出的init_region请求的version = 1
    // 2) split_region的初始version = 0, 完成后version = 1
    int64_t                 _version = 0;
    int64_t                 _table_id = 0;

    // merge 
    pb::RegionInfo          _merge_region_info;

    // BthreadCond
    BthreadCond             _disable_write_cond;
    BthreadCond             _real_writing_cond;
    
    // Legal 
    bthread::Mutex          _legal_mutex;
    bool                    _legal_region = true;
    
    // Restart
    bool                    _restart = false;

    // 计算存储分离开关
    bool                    _storage_compute_separate = false;

    // Raft Service
    braft::Node             _node;
    std::atomic<bool>       _is_leader;
    
    // on_apply的时候更新，可以用来判断快照
    int64_t                 _applied_index = 0;
    // _applied_index
    int64_t                 _data_index = 0;
    int64_t                 _expect_term = -1; 
    
    bool                    _report_peer_info = false;
    std::atomic<bool>       _shutdown;
    bool                    _init_success = false; 
    bool                    _need_decrease = false;
    bool                    _can_heartbeat = false;

    // table line
    std::atomic<int64_t>    _num_table_lines;  // total number of pk record of this region
    std::atomic<int64_t>    _num_delete_lines; // number of deleted rows of last compact
    
    bool                    _removed = false;
    std::string             _rocksdb_start;
    std::string             _rocksdb_end;
    pb::PeerStatus          _region_status = pb::STATUS_NORMAL;


    // learner
    bool                    _is_learner = false;
    bool                    _learner_ready_for_read = false;
    TimeCost                _learner_time;
    
    MetaWriter*             _meta_writer = nullptr;
    bthread::Mutex                  _resource_lock;
    std::shared_ptr<RegionResource> _resource;
    RegionControl           _region_control;
    scoped_refptr<braft::FileSystemAdaptor> _snapshot_adaptor = nullptr;
    
    // 异步执行队列
    ExecutionQueue          _async_apply_log_queue;
    // Snapshot
    int64_t                 _snapshot_num_table_lines = 0; // last snapshot number
    int64_t                 _snapshot_index = 0;           // snapshot index
    TimeCost                _snapshot_time_cost;
    
    TimeCost                _time_cost;                    // 上次收到请求的时间
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
