#include "store/region.h"
namespace TKV {
void Region::construct_heart_beat_request(pb::StoreHBRequest& request, bool need_peer_balance) {
    if (_shutdown || !_can_heartbeat || _removed) {
        return ;
    }
    // TODO: multi-thread cond
    // TODO: 删除大量数据后做compact
    

}

void Region::on_apply(braft::Iterator& iter) {
    // do something
} 

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
