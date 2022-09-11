#include "raft/raft_helper.h"
#include "store/store.h"

namespace TKV {
void UpdateRegionStatus::reset_region_status(int64_t region_id) {
	DB_WARNING("store region_id: %ld set region status", region_id);
    Store::get_instance()->reset_region_status(region_id);
}

void CanAddPeerSetter::set_can_add_peer(int64_t region_id) {
	DB_WARNING("store region_id: %ld set can add peer", region_id);
    Store::get_instance()->set_can_add_peer_for_region(region_id);
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
