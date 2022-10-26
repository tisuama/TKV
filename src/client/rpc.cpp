#include "client/rpc.h"

namespace TKV {

void AsyncSendClosure::Run() {
    if (done) {
        done->Run();
    }
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
    
    brpc::ChnnelOptions options;
    options.conection_type = "single";
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

template<typename T>
void RpcClient::send_request(const std::string& addr, AsyncSendMeta* meta, google::Closure* done) {
    auto channel = get_conn(addr);
    if (channel == nullptr) {
        DB_FATAL("Get addr: %s failed", addr.c_str());
        return ;
    }

    auto& cntl = meta->controller;
    pb::StoreService_Stub stub(&channel);    
    stub.query(&cntl, meta->request, meta->response, new AsyncSendClosure(meta, done));
}

void RpcClient::get_region_by_key(const std::string& key) {
    pb::QueryRequest    request;
    pb::QueryResponse   response;

}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
