#include <string>

#include "client/client_impl.h"
#include "meta/meta_server_interact.h"

namespace TKV {

int ClientImpl::init() {
    _is_inited = true;
    return 0;
}

void ClientImpl::process_request(std::shared_ptr<BatchData> batch_data) {
    CHECK(batch_data);
    // request先根据region进行group分组
    for (auto iter = batch_data->new_iterator(); iter->valid(); iter->next()) {
        auto region_id = iter->region_id();
        auto meta = new AsyncSendMeta(region_id, batch_data);
        auto done = new AsyncSendClosure(meta);
        auto region = _regin_cache->get_region(region_id);
        if (region == nullptr) {
            CHECK("region not found");
        }
        _rpc->send_request(region->leader, meta, done);
    }
}

KeyLocation ClientImpl::locate_key(const std::string& key) {
    return _regin_cache->locate_key(key);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
