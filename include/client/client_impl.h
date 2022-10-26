#pragma once
#include <braft/raft.h>
#include <string>

#include "client/rpc.h"


namespace TKV {
struct RpcSendClosure {
    braft::Closure*     done;
    uint64_t            start_time;
    int                 retry_time;

    virtual void Run();
}; 

class ClientImpl {
public:
    ClientImpl(const std::string& meta_server_bns)
        : _meta_server_bns(meta_server_bns)
        , _cache(std::unqiue_ptr<RegionCache>())
        , _rpc(std::unqiue_ptr<RpcClient>())
    {}

    int init();

    void process_request(AsyncSendMeta* meta, AsyncSendClosure* done);
    void process_response(AsyncSendMeta* meta);
    
    
private: 
    std::string         _meta_server_bns;    
    // std::shared_ptr也行
    MetaServerInteract* _meta_server;
    SmartRegionCache    _cache;
    SmartRpcClient      _rpc;
    bool                _is_inited { false };
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
