#include "client/meta_client.h"
#include "proto/meta.pb.h"
#include "meta/meta_server_interact.h"
#include "common/common.h"

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

int64_t MetaClient::gen_tso() {
    pb::TSORequest  request;
    pb::TSOResponse response;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(1);

    int retry_time = 0;
    int ret = 0;
    for (;;) {
        retry_time++;
        ret = MetaServerInteract::get_instance()->send_request("tso_service", request, response);
        if (ret < 0) {
            if (response.errcode() == pb::RETRY_LATER && retry_time < 5) {
                bthread_usleep(TSO::update_timestamp_interval_ms * 1000LL);
                continue;
            } else {
                DB_FATAL("TSO gen failed, response: %s", response.ShortDebugString().c_str());
                return ret;
            }
        }
        break;
    }
    auto& tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << TSO::logical_bits) + tso.logical();
    return timestamp;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
