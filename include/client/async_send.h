#pragma once
#include "client/backoff.h"
#include "client/cluster.h"
#include "client/region_cache.h"

namespace TKV {

struct AsyncSendMeta {
    AsyncSendMeta(std::shared_ptr<Cluster> cluster, const RegionVerId& region_ver) 
        : cluster(cluster)
        , region_ver(region_ver)
        , region_id(region_ver.region_id)
        , bo(PessimisticLockMaxBackOff)
    {}
    
    ~AsyncSendMeta() {
        if (request) {
            delete request;
        }
        if (response) {
            delete response;
        }
    }
    
    void on_send_failed();
    int  on_region_error();

    // 正常请求
    void send_request();

    std::shared_ptr<Cluster>    cluster;
    brpc::Controller            cntl;
    RegionVerId                 region_ver;
    int64_t                     region_id;
    pb::StoreReq*               request     {nullptr};
    pb::StoreRes*               response    {nullptr};
    uint32_t                    retry_times {0};
    BackOffer                   bo;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
