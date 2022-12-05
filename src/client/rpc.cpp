#include "client/rpc.h"
#include <brpc/channel.h>

namespace TKV {

void OnRPCDone::Run() {
    on_success();
    delete this;
}

/* request 请求成功 */
void OnRPCDone::on_success() {
    auto response = batch_data->get_response(region_id);
    auto dones    = batch_data->get_closure(region_id);

    if (controller.Failed()) {
        DB_WARNING("region_id: %ld send rpc faild, msg: %s", 
                region_id, controller.ErrorText().c_str());
    }
    
    CHECK(dones->size() == 1);
    auto raw_done = static_cast<RawClosure*>(dones->at(0));
    if (raw_done->has_result()) {
        CHECK(response->kv_pairs_size() == 1);
        raw_done->set_result(response->kv_pairs(0).value());
    }
    raw_done->Run();
}

/* request 请求失败 */
void OnRPCDone::on_failed() {
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

    _channels[addr] = channel;
    return channel;
}

void RpcClient::send_request(const std::string& addr, OnRPCDone* done) {
    auto channel = get_conn(addr);
    if (channel == nullptr) {
        DB_FATAL("Get addr: %s failed", addr.c_str());
        return ;
    }

    auto& cntl = done->controller;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    
    auto batch_data = done->batch_data;
    auto region_id  = done->region_id;
    auto request    = batch_data->get_request(region_id);
    auto response   = batch_data->get_response(region_id);

    pb::StoreService_Stub stub(channel);    
    DB_DEBUG("log_id: %lu region_id: %ld request: %s", 
            log_id, region_id, request->ShortDebugString().c_str());
    stub.query(&cntl, request, response, done);
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
