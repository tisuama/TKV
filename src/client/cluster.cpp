#include <string>

#include "client/cluster.h"
#include "meta/meta_server_interact.h"

namespace TKV {

int Cluster::init() {
    /* init meta client */
    int ret = _meta_client->init();
    if (ret < 0) {
        return -1;
    }

    /* set inited */
    _is_inited = true;
    return 0;
}

KeyLocation Cluster::locate_key(const std::string& key) {
    return _region_cache->locate_key(key);
}

int64_t Cluster::gen_tso() {
    return _meta_client->gen_tso();
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
