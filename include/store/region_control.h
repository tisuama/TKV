#pragma once

#include <atomic>
#include <memory>
#include "common/log.h"
#include "proto/meta.pb.h"
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
    
    // static function
    static int remove_data(int64_t drop_region_id);
    static int remove_meta(int64_t drop_region_id);
    static int remove_snapshot_path(int64_t drop_region_id);
    static int clear_all_info_for_region(int64_t drop_region_id);
    static int ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id);
    static int ingest_data_sst(const std::string& data_sst_file, int64_t region_id, bool move_files);

    // called by region
    void sync_do_snapshot();

private:
    Region* _region = nullptr;
    int64_t _region_id = 0;
    std::atomic<pb::RegionStatus> _status;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
