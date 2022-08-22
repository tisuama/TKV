#pragma once

namespace TKV {
class IndexTermMap {
public:
    IndexTermMap() {}

    struct cmp {
        bool operator()(int64_t index, const braft::LogId& log_id) const {
            return index < log_id.index;
        }
    };
private:
    std::deque<braft::LogId> _q;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
