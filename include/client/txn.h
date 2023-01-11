#pragma once
#include "client/cluster.h"
#include <string>

namespace TKV {
const uint32_t TxnCommitBatchSize = 16 * 1024;

struct Txn {
    std::shared_ptr<Cluster> cluster;
    
    std::map<std::string, std::string> buffer;

    // 事务开始的时间戳
    uint64_t start_ts;

    std::chrono::milliseconds start_time;

    explicit Txn(std::shared_ptr<Cluster> cluster)
        : cluster(cluster)
        , start_ts(cluster->meta_client->gen_tso())
        , start_time(butil::gettimeofday_ms())
    {}
    
    void commit() {
        auto committer = std::make_shared<TwoPhaseCommitter>(this);
    }    
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
