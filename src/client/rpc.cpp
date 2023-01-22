#include "client/rpc.h"
#include <brpc/channel.h>

namespace TKV {

brpc::Channel* RpcClient::get_conn(const std::string& addr) {
    BAIDU_SCOPED_LOCK(_mutex);    
    if (_channels.find(addr) == _channels.end()) {
        return create_conn(addr);
    }
    return _channels[addr];
}

brpc::Channel* RpcClient::create_conn(const std::string& addr) {
    brpc::Channel* channel = new brpc::Channel;
    
    brpc::ChannelOptions options;
    options.connection_type = "single";
    options.timeout_ms = 2000;
    options.max_retry = 5;

    if (channel->Init(addr.c_str(), &options) != 0) {
        DB_FATAL("Init channel for addr: %s failed", addr.c_str());
        return nullptr;
    }

    _channels[addr] = channel;
    return channel;
}

template<typename T1, typename T2>
int RpcClient::send_request(const std::string& addr,
    brpc::Controller* cntl,
    const T1* request,
    T2* response,
    google::protobuf::Closure* done) {

    auto channel = get_conn(addr);
    if (channel == nullptr) {
        DB_FATAL("Get channel for addr: %s failed", addr.c_str());
        return -1;
    }
    uint64_t log_id = butil::fast_rand();
    cntl->set_log_id(log_id);
    
    DB_DEBUG("[send] request %s", request.ShortDebugString().c_str());    
    pb::StoreService_Stub stub(&channel);
    stub.query(cntl, request, response, done);

    if (done == NULL && cntl.Failed()) {
        return -1;
    }
    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
