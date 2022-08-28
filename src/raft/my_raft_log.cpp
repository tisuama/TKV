#include "raft/my_raft_log.h"
#include "raft/my_raft_log_storage.h"
#include "raft/my_raft_meta_storage.h"

namespace TKV {
static pthread_once_t g_register_once = PTHREAD_ONCE_INIT;

struct MyRaftExtension {
    MyRaftLogStorage my_raft_log_storage;
    MyRaftMetaStorage my_raft_meta_storage;
};

static void register_once_or_die() {
    static MyRaftExtension* s_ext = new MyRaftExtension;
    braft::log_storage_extension()->RegisterOrDie("myraftlog", &s_ext->my_raft_log_storage);
    braft::meta_storage_extension()->RegisterOrDie("myraftmeta", &s_ext->my_raft_meta_storage);
    DB_WARNING("Register MyRaft Exentension Success");
}

int register_myraft_extension() {
    return pthread_once(&g_register_once, register_once_or_die);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
