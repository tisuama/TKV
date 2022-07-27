#pragma once

#include "common/common.h"

namespace TKV {
class Concurrency {
public:
    static Concurrency* get_instance() {
        static Concurrency instance;
        return &instance;
    }
    
    BthreadCond snapshot_load_concurrency;
    BthreadCond receive_add_peer_concurrency;
    BthreadCond add_peer_concurrency;
    BthreadCond raft_write_concurrency;
    BthreadCond service_write_concurrency;

private:
    Concurrency()
        : snapshot_load_concurrency(-FLAGS_snapshot_load_num)
        , receive_add_peer_concurrency(-FLAGS_snapshot_load_num)
        , add_peer_concurrency(-FLAGS_snapshot_load_num)
        , raft_write_concurrency(-FLAGS_raft_write_concurrency)
        , service_write_concurrency(-FLAGS_service_write_concurrency)
    {}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
