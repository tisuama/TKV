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
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
