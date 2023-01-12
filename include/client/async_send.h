#pragma once
#include "client/cluster.h"

namespace TKV {

struct AsynSendMeta {
    AsynSendMeta(std::shared_ptr<Cluster> cluster, const RegionVerID& region_ver) 
        : cluster(cluster)
        , region_ver(region_ver)
        , region_id(region_ver.region_id)
    {}
    
    ~AsynSendMeta() {
        if (request) {
            delete request;
        }
        if (response) {
            delete response;
        }
    }
    
    void on_send_failed();
    void on_region_error();

    // 正常请求
    void send_request();

    std::shared_ptr<Cluster>    cluster;
    brpc::Controller            cntl;
    RegionVerID                 region_ver;
    int64_t                     region_id;
    pb::StoreRequest*           request     {nullptr};
    pb::StoreResponse*          response    {nullptr};
    uint32_t                    retry_times {0};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
