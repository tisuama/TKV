#pragma once
#include "engine/rocks_wrapper.h"

namespace TKV {

class MetaRocksdb {
public:
    virtual ~MetaRocksdb() {}
    
    static MetaRocksdb* get_instance() {
        static MetaRocksdb db;
        return &db;
    }
    int init();
    int put_meta_info(const std::string& key, const std::string& value);
    int put_meta_info(const std::vector<std::string>& keys,
                      const std::vector<std::string>& values);
    int get_meta_info(const std::string& key, std::string* value);
    int delete_meta_info(const std::vector<std::string>& keys);
    int write_meta_info(const std::vector<std::string>& put_keys,
                        const std::vector<std::string>& put_values, 
                        const std::vector<std::string>& delete_keys);
    
private:
    MetaRocksdb() {}

    RocksWrapper* _rocksdb {nullptr};
    rocksdb::ColumnFamilyHandle* _handle {nullptr};
};

} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
