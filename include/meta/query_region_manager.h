#pragma once

#include "meta/region_manager.h"

namespace TKV {
class QueryRegionManager {
public:
    ~QueryRegionManager() {}
    
    static QueryRegionManager* get_instance() {
        static QueryRegionManager instance;
        return &instance;
    }

    void get_region_info(const pb::MetaReq* request,
            pb::MetaRes* response);

private:
    QueryRegionManager() {}
}; 
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
