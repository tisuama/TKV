#include "store/closure.h"

namespace TKV {
void CovertToSyncClosure::Run() {
    if (!status().ok()) {
        DB_FATAL("region_id: %ld sync step exec fail, status: %s",
                region_id, status().error_cstr());
    } else {
        DB_WARNING("region_id: %ld sync step success", region_id);
    }
    sync_sign.decrease_signal();
    delete this;
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
