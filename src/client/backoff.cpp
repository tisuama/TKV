#include "client/backoff.h"

namespace TKV {
std::shared_ptr<BackOff> NewBackOff(BackOffType type) {
    switch(type) {
        case BoStoreRPC:
            return std::make_shared<BackOff>(100, 2000);
        case BoTxnLock:
            return std::make_shared<BackOff>(200, 3000);
        case BoTxnLockFast:
            return std::make_shared<BackOff>(100, 3000);
        case BoDiskIsFull:
            return std::make_shared<BackOff>(500, 5000);
        case BoMaxTsNotSynced:
            return std::make_shared<BackOff>(2, 500);
        case BoMaxDataNotReady:
            return std::make_shared<BackOff>(100, 2000);
        default:
            assert("not support type");
    }
    return nullptr;
}

int BackOffer::backoff_with_max_sleep(BackOffType type, int max_sleep_time) {
    std::shared_ptr<BackOff> bo;
    auto it = backoff_map.find(type); 
    if (it != backoff_map.end()) {
        bo = it->second;
    } else {
        bo = NewBackOff(type);
        backoff_map[type] = bo;
    }
    total_sleep += bo->sleep(max_sleep_time);
    if (max_sleep_time > 0 && total_sleep > max_sleep_time) {
        return -1;
    }
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
