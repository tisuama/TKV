#include "client/rpc.h"
#include <brpc/channel.h>

namespace TKV {

void AsyncSendClosure::Run() {
    auto batch = meta->batch_data;
    auto closures = batch->closures(
    delete this;
}

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

    BAIDU_SCOPED_LOCK(_mutex);
    _channels[addr] = channel;
    return channel;
}

void RpcClient::send_request(const std::string& addr, AsyncSendMeta* meta, AsyncSendClosure* done) {
    auto channel = get_conn(addr);
    if (channel == nullptr) {
        DB_FATAL("Get addr: %s failed", addr.c_str());
        return ;
    }

    auto& cntl = meta->controller;
    uint64_t log_id =  butil::fast_rand();
    cntl.set_log_id(log_id);

    pb::StoreService_Stub stub(channel);    
    stub.query(&cntl, meta->request, meta->response, new AsyncSendClosure(meta, done));
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
