#include <string>

#include "client/cluster.h"
#include "meta/meta_server_interact.h"

namespace TKV {

int Cluster::init() {
    /* init meta client */
    int ret = meta_client->init();
    if (ret < 0) {
        return -1;
    }

    /* set inited */
    _is_inited = true;
    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
