#pragma once

#include <string>


namespace TKV {
class Conf {
public:
    Conf(std::string& path, int is_meta, int id)
        : _conf_path(path)
        , _is_meta(is_meta)
        , _id(id)
    {}
    
    int parse();
    
private:
    int internal_parse(std::ifstream& stream);

    const std::string _conf_path;
    const int _is_meta;
    int _id;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
