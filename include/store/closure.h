#pragma once
#include "common/common.h"
#include "store/region.h"

namespace TKV {
struct CovertToSyncClosure: public braft::Closure {
    CovertToSyncClosure(BthreadCond& _sync_sign, int64_t _region_id)
        : sync_sign(_sync_sign), region_id(_region_id) 
    {}

    virtual void Run() override;
    BthreadCond& sync_sign;
    int64_t region_id;
};

class DMLClosure: public braft::Closure {
public:
    DMLClosure(): cond(nullptr) { } 
    DMLClosure(BthreadCond* cond): cond(cond) {}
    void Run();

    pb::OpType op_type;
    pb::StoreRes* response = nullptr;
    google::protobuf::Closure* done = nullptr;
    Region* region = nullptr;
    TimeCost cost;
    std::string remote_side;
    BthreadCond* cond;

    bool is_sync = false;
    bool is_seperate = false;
    int64_t txn_num_increase_rows = 0;
    int64_t applied_index = 0;
    uint64_t log_id = 0;
};
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
