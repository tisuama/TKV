#pragma once
#include "common/common.h"
#include "client/meta_client.h"

namespace TKV {
constexpr int physical_bits = 18;
inline int64_t extract_physical(uint64_t ts) {
    return ts >> physical_bits;
}

class Oracle {
public:
    Oracle(std::shared_ptr<MetaClient> meta_client, std::chrono::milliseconds update_interval)
        : meta_client(meta_client)
        , update_interval(update_interval)
    {
        quit = false;

        auto update_fn = [&]() {
            update_ts(update_interval);
        };
        worker.run(update_fn);

        last_ts = 0;
    }

    ~Oracle() {
        quit = true;
        worker.join();
    }

    int64_t util_expired(uint64_t lock_ts, uint64_t ttl) {
        return extract_physical(lock_ts) + ttl - extract_physical(last_ts);
    }

    uint64_t get_low_resolution_ts() {
        return last_ts;
    }

    bool is_expired(uint64_t lock_ts, uint64_t ttl) {
        return util_expired(lock_ts, ttl) <= 0;
    }

private:
    void update_ts(std::chrono::milliseconds update_interval) {
        for (;;) {
            if (quit) {
                return ;
            }
            auto ts = meta_client->gen_tso();
            if (ts <= 0) {
                DB_WARNING("update tso eror");  
            } else {
                last_ts = ts;
            }
            bthread_usleep(update_interval.count() * 1000LL);
        }
    }

public:
    std::shared_ptr<MetaClient> meta_client;
    std::atomic_bool            quit;
    std::atomic<uint64_t>       last_ts;
    std::chrono::milliseconds   update_interval;
    Bthread                     worker;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
