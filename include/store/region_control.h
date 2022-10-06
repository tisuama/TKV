#pragma once

#include <atomic>
#include <memory>
#include "common/log.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "common/common.h"

namespace TKV {
class Region;
class RegionControl {
typedef std::shared_ptr<Region> SmartRegion;
public:
    RegionControl(Region* region, int64_t region_id)
        : _region(region)
        , _region_id(region_id)
    {}

    virtual ~RegionControl() {}

    void set_status(const pb::RegionStatus& status) {
        _status.store(status);
    }
    
    pb::RegionStatus get_status() const {
        return _status.load();
    }

    void reset_region_status() {
        pb::RegionStatus expect_status = pb::DOING;
        if (!_status.compare_exchange_strong(expect_status, pb::IDLE)) {
            DB_WARNING("region_id: %ld region status is not DOING, do nothing", _region_id);
        }
    }
    
    // static function
    static int remove_data(int64_t drop_region_id);
    static int remove_meta(int64_t drop_region_id);
    static int remove_log_entry(int64_t drop_region_id);
    static int remove_snapshot_path(int64_t drop_region_id);
    static int clear_all_info_for_region(int64_t drop_region_id);
    static int ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id);
    static int ingest_data_sst(const std::string& data_sst_file, int64_t region_id, bool move_files);

    static void compact_data_in_queue(int64_t region_id);
    static void compact_data(int64_t region_id);

    // called by region
    void sync_do_snapshot();
    
    void add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue);

private:
    int legal_for_add_peer(const pb::AddPeer& add_peer, pb::StoreRes* response);
    void construct_init_region_request(pb::InitRegion& init_request); 
    void node_add_peer(const pb::AddPeer& add_peer, 
                       const std::string& new_instance,
                       pb::StoreRes* response,
                       google::protobuf::Closure* done);
    int init_region_to_store(const std::string& instance_address, 
                             const pb::InitRegion& init_region_request,
                             pb::StoreRes* store_response); 
                

private:
    Region* _region = nullptr;
    int64_t _region_id = 0;
    std::atomic<pb::RegionStatus> _status;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
