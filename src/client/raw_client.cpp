#include "client/raw_client.h"
namespace TKV {

int RawKVClient::init() {
    int ret = init_log("client");
    if (ret < 0) {
        return -1;
    }

    ret = _kv->init();
    if (ret < 0) {
        DB_WARNING("ClientImpl init failed");
        return -1;
    }
    
    DB_WARNING("Raw KV Client init success");
    return 0;
}

int RawKVClient::put(const std::string& key,
         const std::string& value) {
    auto batch_data = std::make_shared<BatchData>();
    auto key_location = _kv->locate_key(key);
    auto sync_done = new SyncClosure;
    batch_data->put(key, value, key_location, new RawClosure(nullptr, sync_done));
    _kv->process_request(batch_data);

    sync_done->wait();
    if (sync_done->status().ok()) {
        return 0;
    }
    return -1;
}

int RawKVClient::get(const std::string& key, 
         std::string* value) {
    auto batch_data = std::make_shared<BatchData>();
    auto key_location = _kv->locate_key(key);
    auto sync_done = new SyncClosure;
    batch_data->get(key, value, key_location, new RawClosure(value, sync_done));
    _kv->process_request(batch_data);

    sync_done->wait();
    if (sync_done->status().ok()) {
        return 0;
    }
    return -1;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
