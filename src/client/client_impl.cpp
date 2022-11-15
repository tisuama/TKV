#include <string>

#include "client/client_impl.h"
#include "meta/meta_server_interact.h"

namespace TKV {

int ClientImpl::init() {
    /* init meta client */
    int ret = _meta_client->init();
    if (ret < 0) {
        return -1;
    }

    /* set inited */
    _is_inited = true;
    return 0;
}

void ClientImpl::process_request(std::shared_ptr<BatchData> batch_data) {
    CHECK(batch_data);
    /* request先根据region进行group分组 */
    for (auto iter = batch_data->new_iterator(); iter->valid(); iter->next()) {
        auto region_id = iter->region_id();
        /* new AsyncSendMeta for region */
        auto meta = new AsyncSendMeta(region_id, batch_data);

        /* new AsyncSendClosure for region */
        auto done = new AsyncSendClosure(meta);
        
        DB_DEBUG("region_id: %ld meta: %p, done: %p, version: %p", 
                region_id, meta, done, iter->version());

        auto region = _region_cache->get_region(*iter->version());
        if (region == nullptr) {
            CHECK("region not found");
        }
        _rpc_client->send_request(region->leader, meta, done);
    }
}

KeyLocation ClientImpl::locate_key(const std::string& key) {
    return _region_cache->locate_key(key);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
