#pragma once

#include <string>
#include <map>
#include <cmath>
#include "common/common.h"

namespace TKV {
constexpr int PessimisticLockMaxBackOff = 20000; // ms

enum BackOffType {
    BoStoreRPC = 0,
    BoTxnLock,
    BoTxnLockFast,
    BoMetaRPC,
    BoRegionMiss,
    BoServerBusy,
    BoTxnNotFound,
    BoMaxTsNotSynced,
    BoMaxDataNotReady,
    BoRegionScheduling,
    BoDiskIsFull
};

inline int expo(int base, int cap, int n) {
    return std::min(double(cap), double(base) * std::pow(2.0, double(n)));
}

struct BackOff {
    int base;
    int cap;
    int retry_times;

    BackOff(int base, int cap) 
        : base(base)
        , cap(cap)
        , retry_times(0)
    {
        if (base < 2) {
            base = 2;
        }
    }

    int sleep(int max_sleep_time /* ms */) {
        // FullJitter
        int sleep_time = expo(base, cap, retry_times);
        bthread_usleep(sleep_time * 1000LL);
        retry_times++;
        return sleep_time;
    }
};

extern std::shared_ptr<BackOff> NewBackOff(BackOffType type);

struct BackOffer {
    std::map<BackOffType, std::shared_ptr<BackOff>> backoff_map;
    int total_sleep; // ms
    int max_sleep;   // ms
    
    explicit BackOffer()
        : total_sleep(0)
        , max_sleep(0)
    {}

    explicit BackOffer(int max_sleep)
        : total_sleep(0)
        , max_sleep(max_sleep)
    {}

    void backoff(BackOffType type) {
        backoff_with_max_sleep(type, -1);
    }

    int backoff_with_max_sleep(BackOffType type, int max_sleep_time);
};

} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
