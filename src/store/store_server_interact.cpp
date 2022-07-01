#include "store/store_server_interact.h"

namespace TKV {
DEFINE_int32(store_request_timeout, 60000, "store server request timeout");
DEFINE_int32(store_connect_timeout, 5000, "store connect request timeout");
} // namespace TKV
