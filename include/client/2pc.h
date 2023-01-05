#pragma once
#include <string>
#include <memory>
#include <unordered_map>

namespace TKV {

struct TwoPhaseCommitter: public std::enable_shared_from_this<TwoPhaseCommitter> {
public:
private:
    std::unordered_map<std::string, std::string> mutations;
    std::vector<std::string> keys;
    
    uint64_t start_ts  {0};

    bthread_mutex_t commit_mutex;
    uint64_t commit_ts {0};
    uint64_t min_commit_ts {0};
    uint64_t max_commit_ts {0};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
