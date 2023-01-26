#include "client/async_send.h"
#include "client/backoff.h"

namespace TKV {

void AsyncSendMeta::on_send_failed() {
    cluster->region_cache->drop_region(region_ver);
}

int AsyncSendMeta::on_response_failed() {
    auto code = response.errcode();
    switch(code) {
        case pb::NOT_LEADER: {
            if (response.has_leader()) {
                DB_DEBUG("region_id: %ld request: %s, NOT LEADER, redirect to: %s",
                        region_id, request.ShortDebugString().c_str(), response.leader().c_str());
                // 如果leader合法，则不等待
                if (!cluster->region_cache->update_leader(region_ver, response.leader())) {
                    bo.backoff(BoRegionScheduling);
                }
            } else {
                // 可能leader和peer网络间隔，或者raft group正在选举
                cluster->region_cache->drop_region(region_ver);
                bo.backoff(BoRegionScheduling);
            }
            return 0;
        }
        case pb::DISK_IS_FULL: {
            bo.backoff(BoDiskIsFull);
            return 0;
        }
        case pb::SERVER_IS_BUSY: {
            bo.backoff(BoServerBusy);
            return 0;
        }
        case pb::STALE_COMMAND: 
        case pb::RAFT_ENTRY_TOO_LARGE: {
            return -1;
        }
        case pb::MAX_TIMESTAMP_NOT_SYNCED: {
            bo.backoff(BoMaxTsNotSynced);
            return 0;
        }
        case pb::READ_INDEX_NOT_READY: {
            // region正在split或者merge，read_index没法及时处理
            bo.backoff(BoRegionScheduling);
            return 0;
        } 
        case pb::MAX_DATA_NOT_READY: {
            // 过期请求发送到peer，数据还没准备好
            bo.backoff(BoMaxDataNotReady);
            return 0;
        }
    }
    cluster->region_cache->drop_region(region_ver);
    return -1;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
