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
    DB_DEBUG("region_id: %ld DMLClosure done run, response: %s, done: %p",  
            region_id, response->ShortDebugString().c_str(), done);
    if (done) {
        done->Run();
    }
    delete this;

}

void AddPeerClosure::Run() {
    if (!status().ok()) {
        DB_WARNING("region_id: %ld ADD_PEER failed, new_instance: %s, status: %s",
                region->get_region_id(), status().error_cstr(), new_instance.c_str());
        if (response) {
            ERROR_SET_RESPONSE_FAST(response, pb::NOT_LEADER, "Not Leader", 0);
            response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
        }
    } else {
        DB_WARNING("region_id: %ld ADD_PEER success, cost: %ld", 
                region->get_region_id(), cost.get_time());
    }
    if (!is_split) {
        region->reset_region_status();
    }
    DB_WARNING("region_id: %ld region status is reset", region->get_region_id());
    if (done) {
        done->Run();
    }
    cond.decrease_signal();
    delete this;
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
