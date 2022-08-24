#pragma once
#include <braft/log_entry.h>
#include "common/common.h"

namespace TKV {
class IndexTermMap {
public:
    IndexTermMap() {}

    struct cmp {
        bool operator()(int64_t index, const braft::LogId& log_id) const {
            return index < log_id.index;
        }
    };
    
    int64_t get_term(int64_t index) const {
        if (_q.empty()) {
            return 0;
        }
        if (index > _q.back().index) {
            return _q.back().term;
        }
        if (_q.size() < 15ul) {
            for (auto it = _q.rbegin(); it != _q.rend(); ++it) {
                if (index >= iter->index) {
                    return iter->term;
                }
            }
        } else {
            auto it = std::upper_bound(_q.begin(), _q.end(), index, cmp());
            if (it == _q.begin()) {
                return 0;
            }
            --iter;
            return iter->term;
        }
        return 0;
    }


    int append(const braft::LogId& log_id) {
        if (_q.empty()) {
            _q.push_back(log_id);
            return 0;
        }
        if (log_id.index <= _q.back().index || log_id.term < _q.back().term()) {
            DB_FATAL("Invalid log_id, index: %ld, term: %ld, q.back(), index: %ld, term: %ld",
                    log_id.index, log_id.term, q.back().index, q.back().term);
            return -1;
        }
        if (log_id.term != _q.back().term) {
            _q.push_back(log_id);
        }
        return 0;
    }

    void truncate_prefix(int64_t first_log_index) {
        if (_q.empty()) {
            DB_WARNING("term map has no log_id, first_log_index: %ld", first_log_index);
            return 0;
        }
        size_t num_pop = 0;
        for (auto it = _q.begin(); it != q.end(); it++) {
            if (it->index > first_log_index) {
                num_pop = it - _q.begin();
            }
        }
        _q.erase(_q.begin(), _q.end() + num_pop);
    }

    void truncate_suffix(int64_t last_log_index) {
        while (!_q.empty() && _q.back().index > last_log_index) {
            _q.pop_back();
        }
    }
    
    void rest() {
        _q.clear();
    }

private:
    std::deque<braft::LogId> _q;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
