#pragma once
#include "common/common.h"

namespace TKV {
class StatisticsInfo {
public: 
   StatisticsInfo() {}

private:
    int64_t     _table_id {0};
    int64_t     _version {0};

};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
