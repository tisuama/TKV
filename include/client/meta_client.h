#pragma once

namespace TKV {
class MetaClient {
public:
    MetaClient(const std::string& meta_bns)
        : _meta_server_bns(meta_bns)
    {}
    void init();
    
    int reload_region(std::vector<pb::RegionInfo>& region_infos);

private:
    std::string     _meta_server_bns;
    // TODO: namespace, database
    std::string     _table_name;
    bool            _is_inited { false }
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
