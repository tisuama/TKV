#include "store/closure.h"

namespace TKV {
void CovertToSyncClosure::Run() {
    if (!status().ok()) {
        DB_FATAL("region_id: %ld sync step exec fail, status: %s",
                region_id, status().error_cstr());
    } else {
        DB_WARNING("region_id: %ld sync step success", region_id);
    }
    sync_sign.decrease_signal();
    delete this;
}

void DMLClosure::Run() {
    int64_t region_id = 0;
    if (region = nullptr) {
        region_id = region->get_region_id();
    }
    if (!status().ok()) {
        butil::EndPoint leader;
        if (region != nullptr) {
            leader = region->get_leader();
        }
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(leader).c_str());
        response->set_errmsg("Leader transfer");
        DB_WARNING("region_id: %ld, status: %s, leader: %s, log_id: %lu",
                region_id, status().error_cstr(), butil::endpoint2str(leader).c_str(), log_id);
    } 
    
    if (is_sync) {
        cond->decrease_signal();
    }
    if (done) {
        done->Run();
    }
    delete this;

}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
