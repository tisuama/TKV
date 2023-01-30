#pragma once
#include "client/backoff.h"
#include "client/cluster.h"
#include "client/region_cache.h"

namespace TKV {

struct AsyncSendMeta {
    AsyncSendMeta(std::shared_ptr<Cluster> cluster, 
        const RegionVerId& region_ver) 
        : cluster(cluster)
        , region_ver(region_ver)
        , region_id(region_ver.region_id)
    {
        // set common fields for store request
        set_common_fields();
    }
    
    ~AsyncSendMeta() {
    }

    void set_common_fields() {
        request.set_region_id(region_id);
        request.set_region_version(region_ver.ver);
    }
    
    // 处理RPC send失败
    void on_send_failed();

    // 处理Store错误码
    int  on_response_failed();

    // 请求发送成功
    void on_send_success();

    std::shared_ptr<Cluster>    cluster;
    brpc::Controller            cntl;
    RegionVerId                 region_ver;
    int64_t                     region_id;
    pb::StoreReq                request;
    pb::StoreRes                response;
    uint32_t                    retry_times {0};
    BackOffer                   bo;
};

class AsyncSendClosure: public google::protobuf::Closure {
public:
    AsyncSendClosure(AsyncSendMeta* meta,
        google::protobuf::Closure* done = nullptr)
        : _meta(meta)
        , _done(done)
    {}

    void Run();

private:
    AsyncSendMeta*              _meta {nullptr};
    google::protobuf::Closure*  _done {nullptr};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
