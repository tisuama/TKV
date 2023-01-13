#pragma once
#include <string>
#include <memory>

#include "client/rpc.h"
#include "client/meta_client.h"
#include "client/region_cache.h"
#include "client/oracle.h"

namespace TKV {
constexpr int oracle_update_interval = 2000;

class Cluster {
public:
    // meta_server_bns: meta_server地址
    // table_name: 请求的表的资源
    Cluster(const std::string& meta_server_bns, const std::string& table_name)
        : meta_client(std::make_shared<MetaClient>(meta_server_bns, table_name))
        , region_cache(std::make_shared<RegionCache>(meta_client))
        , rpc_client(std::make_shared<RpcClient>())
        , oracle(std::make_shared<Oracle>(meta_client, 
                    std::chrono::milliseconds(oracle_update_interval)))
    {}

    int init(); 
    
    int send_request(const pb::StoreReq* request, 
                     pb::StoreRes* response,
                     brpc::Controller* cntl,
                     const std::string& addr,
                     ::google::protobuf::Closure* done);

public:
    // std::shared_ptr也行
    std::shared_ptr<MetaClient>  meta_client;
    std::shared_ptr<RegionCache> region_cache;
    std::shared_ptr<RpcClient>   rpc_client;
    std::shared_ptr<Oracle>      oracle;
    bool                _is_inited { false };
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
