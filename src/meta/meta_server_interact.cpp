#include "meta/meta_server_interact.h"

namespace TKV {
DEFINE_int32(meta_request_timeout, 30000, "meta server request timeout, default: 30000ms");
DEFINE_int32(meta_connect_timeout, 5000, "meta server connntect timeout, default: 5000ms");
DEFINE_int64(time_between_meta_connect_error_ms, 0, "time between meta error ms");
DECLARE_string(meta_server_bns);
int MetaServerInteract::init() {
    return init_internal(FLAGS_meta_server_bns);
}

int MetaServerInteract::init_internal(const std::string& meta_bns) {
    _master_leader_address.ip = butil::IP_ANY;
    _master_leader_address.port = 0;
    _connect_timeout = FLAGS_meta_connect_timeout;
    _request_timeout = FLAGS_meta_request_timeout;

    brpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = FLAGS_meta_request_timeout;
    channel_opt.connect_timeout_ms = FLAGS_meta_connect_timeout;
    std::string meta_server_addr = meta_bns;
    
    // list or bns
     if (meta_bns.find(":") == std::string::npos) {
        meta_server_addr = std::string("bns://") + meta_bns;
    } else {
        meta_server_addr = std::string("list://") + meta_bns;
    }
    if (_bns_channel.Init(meta_server_addr.c_str(), "rr", &channel_opt)) {
        DB_FATAL("meta server bns pool init failed, bns name: %s", meta_server_addr.c_str());
        return -1;
    }
    _is_init = true;
    return 0;

}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
