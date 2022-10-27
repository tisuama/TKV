#include "client/meta_client.h"
#include "proto/meta.pb.h"
#include "meta/meta_server_interact.h"

namespace TKV {

void MetaClient::init() {
    auto meta = MetaServerInteract::get_instance();
    meta->init_internal(_meta_server_bns);
    is_inited = true;
}

int MetaClient::reaload_region(std::vector<pb::RegionInfo>& region_infos) {
    pb::MetaReq request;
    pb::MetaRes response;

    request.set_op_type(QUERY_REGION);
    request.set_table_name(_table_name);
    int ret = meta->send_request("query", &request, &response);    
    if (!ret) {
        for (auto& info: response.region_infos()) {
            region_info.push_back(info);
        } 
    }
    return ret;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
