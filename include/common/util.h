#pragma once
#include <string> 

namespace TKV {
void split_string(std::vector<std::string>& ret, std::string src, char split_char) {
    size_t  pos = 0, sz = src.size();
    while (pos < sz) {
        size_t s_pos = src.find(split_char);
        if (s_pos != std::string::npos) {
            ret.push_back(src.substr(pos, s_pos - pos)); 
            pos = s_pos + 1;
        } else {
            break;
        }
    }
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
