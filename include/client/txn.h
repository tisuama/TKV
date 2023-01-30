#pragma once
#include "client/cluster.h"
#include "client/2pc.h"
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

    void set(const std::string& key, const std::string& value) {
        buffer.emplace(key, value);
    }

    std::pair<std::string, bool> get(const std::string& key) {
        auto it = buffer.find(key);
        if (it != buffer.end()) {
            return std::make_pair(it->second, true);
        }

        return std::make_pair("", false);
        // TODO: 快照读
        // Snapshot snapshot(cluster, start_ts);
        // std::string value = snapshot.Get(key);
        // if (value.empty()) {
        //     return std::make_pair("", false);
        // }
        // return std::make_pair(value, true);
    }
    
    int commit() {
        auto committer = std::make_shared<TwoPhaseCommitter>(this);
        return committer->execute();
    }    

    void walk_buffer(std::function<void(const std::string&, const std::string&)> fn) {
        for (auto& it: buffer) {
            fn(it.first, it.second);
        }
    }
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
