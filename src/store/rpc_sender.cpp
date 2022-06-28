#include "store/rpc_sender.h"

namespace TKV {
int RpcSender::send_no_op_request(const std::string& instance, 
        int64_t recevie_region_id,
        int64_t request_version) {
    for (int i = 0; i < 5; i++) {
        // TODO: send no op request
    }
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
