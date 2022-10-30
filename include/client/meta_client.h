#pragma once

#include <string>
#include <vector>

#include "proto/meta.pb.h"

namespace TKV {
class MetaClient {
public:
    MetaClient(const std::string& meta_bns, const std::string& table_name)
        : _meta_server_bns(meta_bns)
        , _table_name(table_name)
    {}

    void init();
    
    int reload_region(std::vector<pb::RegionInfo>& region_infos);

private:
    std::string     _meta_server_bns;
    // TODO: namespace, database
    std::string     _table_name;
    bool            _is_inited { false };
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
