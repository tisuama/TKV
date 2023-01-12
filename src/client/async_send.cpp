#include "client/async_send.h"
namespace TKV {

void AsyncSendMeta::on_send_failed() {
    cluster->region_cache->drop_region(region_id);
}

void AsyncSendMeta::on_region_error() {
    auto code = response.errcode();
    if (code == pb::NOT_LEADER) {
        if (response->has_leader()) {
            DB_DEBUG("region_id: %ld request: %s, NOT LEADER, redirect to: %s",
                    region_id, request->ShortDebugString().c_str(), response.leader().c_str());
            if (!cluster->region_cache->update_leader(response->leader())) {
                bo.backoff(bRegionScheduling);
            }
        }
    }
    // TODO: solve error
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
