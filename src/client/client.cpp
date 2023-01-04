#include "client/client.h"
#include "client/raw_client.h"


namespace TKV {
std::shared_ptr<Client> NewRawKVClient(std::shared_ptr<Cluster> cluster) {
    auto client = std::make_shared<RawKVClient>(cluster);
    if (client == nullptr) {
        DB_FATAL("New RawKVClient failed");
        return nullptr;
    }
    return client;
}

std::shared_ptr<Cluster> NewCluster(const std::string& meta_server_bns,
        const std::string& table_name) {
    auto cluster = std::make_shared<Cluster>(meta_server_bns, table_name);
    if (cluster == nullptr) {
        DB_FATAL("New Cluster failed, meta_server_bns: %s", meta_server_bns.c_str());
        return nullptr;
    }
    /* Cluster init */
    if (cluster->init()) {
        DB_FATAL("Cluster init failed, exit");
        return nullptr;
    }
    return cluster;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
