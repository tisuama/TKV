#include "meta/query_region_manager.h"
#include "meta/region_manager.h"

namespace TKV {

void QueryRegionManager::get_region_info(const pb::MetaReq* request,
        pb::MetaRes* response) {
    auto region_manager = RegionManager::get_instance();
    if (request->region_ids_size() == 0) {
        int64_t table_id = request->table_id();
        std::vector<SmartRegionInfo> region_infos;
        region_manager->traverse_region_map([&region_infos, &table_id](SmartRegionInfo& region_info) {
            if (table_id == 0 || table_id == region_info->table_id()) {
                region_infos.push_back(region_info);
            }
        });
        for (auto& region_info: region_infos) {
            auto region_pb = response->add_region_infos();
            // copy to response
            *region_pb = *region_info;
        }
    } else {
        for (int64_t region_id: request->region_ids()) {
            SmartRegionInfo region_info = region_manager->_region_info_map.get(region_id);
            if (region_info != nullptr) {
                auto region_pb = response->add_region_infos();
                *region_pb = *region_info;
            } else {
                response->set_errmsg("region info not exist");
                response->set_errcode(pb::REGION_NOT_EXIST);
            }
        }
    }
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
