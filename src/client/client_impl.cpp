#pragma once
#include <string>
#include "client/client_impl.h"
#include "meta/meta_server_interact.h"

namespace TKV {
int ClientImpl::init() {
    _meta_server = MetaServerInteract::get_instance();
    if (!_meta_server->is_inited()) {
        _meta_server->init_internal(_meta_server_bns);
    }
    _sender = RpcClient::get_instance();
    _is_inited = true;
}

void ClientImpl::process_request(AsyncSendMeta* meta, AsyncSendClosure* done) {
}

void ClientImpl::process_response(AsyncSendMeta* meta) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
