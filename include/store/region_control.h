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

    // called by region
    void sync_do_snapshot();

private:
    Region* _region = nullptr;
    int64_t _region_id = 0;
    std::atomic<pb::RegionStatus> _status;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
