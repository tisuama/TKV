#include "client/client.h"
#include "client/raw_client.h"


namespace TKV {
std::shared_ptr<Client> NewRawKVClient(const std::string& meta_server_bns, 
    const std::string& table_name) {
    auto client = std::make_shared<RawKVClient>(meta_server_bns, table_name);
    if (client == nullptr) {
        DB_FATAL("New RawKVClient failed, meta_server_bns: %s", meta_server_bns.c_str());
        return nullptr;
    }
    return client;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
