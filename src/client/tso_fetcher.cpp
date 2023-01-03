#include "client/tso_fetcher.h"
#include "common/common.h"
#include "meta/meta_server_interact.h"

namespace TKV {
int64_t TSOFetcher::gen_tso() {
    pb::TSORequest  request;
    pb::TSOResponse response;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(1);

    int retry_time = 0;
    int ret = 0;
    for (;;) {
        retry_time++;
        ret = MetaServerInteract::get_instance()->send_request("tso_service", request, response);
        if (ret < 0) {
            if (response.errcode() == pb::RETRY_LATER && retry_time < 5) {
                bthread_usleep(TSO::update_timestamp_interval_ms * 1000LL);
                continue;
            } else {
                DB_FATAL("TSO gen failed, response: %s", response.ShortDebugString().c_str());
                return ret;
            }
        }
        break;
    }
    auto& tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << TSO::logical_bits) + tso.logical();
    return timestamp;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

