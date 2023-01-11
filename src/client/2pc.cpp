#include "client/2pc.h"

namespace TKV {
int64_t send_txn_heart_beat(std::shared_ptr<Cluster> cluster, 
                             const std::string& primary_key, 
                             uint64_t start_ts, 
                             uint64_t ttl) {
    auto location = cluster->region_cache->locate_key(primary_key);

    pb::TxnHeartBeatRequest request;
    request.set_primary_lock(primary_key);
    request.set_start_version(start_ts);
    request.set_advise_lock_ttl(ttl);

    pb::TxnHeartBeatResponse;
    int ret = cluster->rpc_client->send_req_to_region(request, responose);
    if (ret < 0) {
        return ret;
    }
    return responose->lock_ttl();
}

int64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
    if (state.load(std::memory_order_relaxed) == StateClosed) {
        return ;
    }
    std::this_thread::sleep_for(std::chrono::millisecond(LockTTL / 2));
    uint64_t now = committer->cluster->oracle_client->get_low_resolution_ts();        
    uint64_t uptime = extract_physical(now) - extract_physical(committer->start_ts);
    uint64_t new_ttl = uptime + LockTTL;

    int ret = send_txn_heart_beat(committer->cluster, committer->primary_lock, committer->start_ts, new_ttl);
    if (ret < 0) {
        DB_WARNING("txn heartbeat failed, exit now");
        return ;
    }
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
