#include "store/region_control.h"
#include "store/rpc_sender.h"
#include "store/store.h"
#include "store/closure.h"

namespace TKV {
void RegionControl::sync_do_snapshot() {
    DB_WARNING("region_id: %ld sync do snapshot start", _region_id);
    std::string address = Store::get_instance()->address();
    butil::EndPoint leader = _region->get_leader();
    if (leader.ip != butil::IP_ANY) {
        address = butil::endpoint2str(leader).c_str();
    }
    auto ret = RpcSender::send_no_op_request(address, _region_id, _region->_region_info.version());
    if (ret < 0) {
        DB_WARNING("send no op fail, region_id: %ld", _region_id);
    }
    BthreadCond sync_sign;
    sync_sign.increase();
    CovertToSyncClosure* done = new CovertToSyncClosure(sync_sign, _region_id);    
    if (!_region->is_learner()) {
        _region->_node.snapshot(done);
    } else {
        // TODO: snapshot for learner node
        CHECK(false && "learner snapshot not impl");
    }
    sync_sign.wait();
    DB_WARNING("region_id: %ld sync do snapshot success", _region_id);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
