#include <vector>
#include <unordered_map>
#include <memory>

#include "common/latch.h"
#pragma once

namespace TKV {
// 事务的内存数据结构
class Txn {
public:
    Txn(uint64_t start_ts, uint64_t lock_ttl, std::string primary)
        : _start_ts(start_ts)
        , _lock_ttl(lock_ttl)
        , _primary_lock(primary)
    {}

    
    void pwrite(const pb::Mutation& m, const std::string& primary);

    void commit(const std::string& key, uint64_t commit_ts);

private:
    std::vector<std::string>                    _keys;
    std::unordered_map<std::string, std::string> _mutations;
    // start_ts可以作为txn_id使用
    uint64_t                 _start_ts;
    uint64_t                 _lock_ttl;
    std::string              _primary_lock;
    bool                     _committed;
    uint64_t                 _commit_ts        {0};
    bool                     _use_async_commit {false};
    uint64_t                 _txn_size         {0};
    uint64_t                 _min_commit_ts;
    uint64_t                 _max_commit_ts;
    std::shared_ptr<Latches> _latches;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
