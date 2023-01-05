#include "client/raw_client.h"
namespace TKV {

int RawKVClient::init() {
    int ret = init_log("client");
    if (ret < 0) {
        return -1;
    }

    DB_WARNING("Raw KV Client init success");
    return 0;
}

int RawKVClient::put(const std::string& key,
         const std::string& value) {
      
    return 0;
}

int RawKVClient::get(const std::string& key, 
         std::string* value) {

    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
