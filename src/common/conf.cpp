#include "common/conf.h"
#include "common/log.h"
#include <fstream>
#include <butil/strings/string_piece.h>
#include <butil/strings/string_split.h>
#include <gflags/gflags.h>

namespace TKV {
using butil::StringPiece;

int Conf::parse() {
    DB_DEBUG("parse conf path: %s", _conf_path.c_str());
    std::ifstream infile(_conf_path);    
    if (!infile.is_open()) {
        return -1;
    }
    std::string line;
    while (std::getline(infile, line)) {
        auto line_str = StringPiece(line.data());
        line_str.trim_spaces(); 
        // 注释内容
        if (line_str.starts_with("//")) {
            continue;
        }
        // 配置文件
        if (line_str.front() == '[' && 
                line_str.back() == ']') {
            line_str = line_str.substr(1, line_str.size() - 2);
            std::vector<StringPiece> split_vec;
            butil::SplitString(line_str, '.', &split_vec);
            if (split_vec[0].compare("TKV") != 0) {
                infile.close();
                return -1;
            }
            if (split_vec[1].compare("global") == 0) {
                // if gobal config error
                int res = internal_parse(infile);
                if (res) {
                    return -1;
                }
                continue;
            }
            if (_is_meta && split_vec[1].compare("meta") == 0) {
                int id = std::stoi(split_vec[2].data());
                if (id == _id) {
                    return internal_parse(infile);
                }
            }
            if (!_is_meta && split_vec[1].compare("store") == 0) {
                int id = std::stoi(split_vec[2].data());
                if (id == _id) {
                    return internal_parse(infile);
                }
            }
        }
    }
    infile.close();
    return 0;
}

int Conf::internal_parse(std::ifstream& infile) {
    std::string line;
    while (std::getline(infile, line)) {
        auto line_str = StringPiece(line);        
        line_str.trim_spaces();
        if (line_str.size() <= 2) {
            break;
        }
        if (line_str.front() == '[' &&
                line_str.back() == ']') {
            CHECK(0 && "config error");
        }
        std::vector<StringPiece> split_vec;
        butil::SplitString(line_str, '=', &split_vec);
        if (split_vec.size() != 2) {
            infile.close();
            return -1;
        }
        DB_DEBUG("parse conf %s = %s", 
                split_vec[0].as_string().data(), split_vec[1].as_string().data());     
        google::SetCommandLineOption(split_vec[0].as_string().data(), 
                split_vec[1].as_string().data());
    }
    return 0;
}
} // namespce TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
