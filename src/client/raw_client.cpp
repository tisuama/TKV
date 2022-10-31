#include "client/raw_client.h"
namespace TKV {

int RawKVClient::init() {
    DB_WARNING("Raw KV Client init success");
    return 0;
}

void RawKVClient::put(const std::string& key,
         const std::string& value) {
    auto batch_data = std::make_shared<BatchData>();
    auto key_location = _kv->locate_key(key);
    auto sync_done = new SyncClosure;
    batch_data->put(key, value, key_location, new ClientClosure(done));
    _kv->process_request(batch_data);

    sync_done->wait();
}

void RawKVClient::get(const std::string& key, 
         std::string* value) {
    auto batch_data = std::make_shared<BatchData>();
    auto key_location = _kv->locate_key(key);
    auto sync_done = new SyncClosure;
    batch_data->get(key, value, key_location, new GetClosure(value, done));
    _kv->process_request(batch_data);

    sync_done->wait();
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
