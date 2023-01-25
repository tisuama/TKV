#pragma once
#include "client/cluster.h"
#include <string>

namespace TKV {

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

    void walk_buffer(std::function<void(const std::string&, const std::string&)> fn) {
        for (auto& it: buffer) {
            fn(it.first, it.second);
        }
    }
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
