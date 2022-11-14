#include "client/meta_client.h"
#include "proto/meta.pb.h"
#include "meta/meta_server_interact.h"

namespace TKV {

int MetaClient::init() {
    /* init meta client */
    auto meta = MetaServerInteract::get_instance();
    int ret = meta->init_internal(_meta_server_bns);
    if (ret < 0) {
        return -1;
    }

    DB_DEBUG("meta client is inited with meta_bns: %s", _meta_server_bns.c_str());
    _is_inited = true;
    return 0;
}

int MetaClient::reload_region(std::vector<pb::RegionInfo>& region_infos) {
    pb::MetaReq request;
    pb::MetaRes response;

    request.set_op_type(pb::QUERY_REGION);
    request.set_table_name(_table_name);

    DB_DEBUG("meta query: %s", request.DebugString().c_str());
    
    auto meta = MetaServerInteract::get_instance();
    int ret = meta->send_request("query", request, response);    
    if (!ret) {
        DB_DEBUG("reload region response: %s", response.ShortDebugString().c_str());
        for (auto& info: response.region_infos()) {
            region_infos.push_back(info);
        } 
    }
    return ret;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
