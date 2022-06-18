#pragma once
#include <google/protobuf/descriptor.h>
#include "proto/store.pb.h"
#include "common/common.h"

namespace TKV {
DECLARE_int32(store_request_timeout);
DECLARE_int32(store_connect_timeout);
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
