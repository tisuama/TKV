#pragma once

#include <string>

namespace TKV {
class TSOFetcher {
public:
    static int64_t gen_tso();
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
