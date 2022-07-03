#pragma once

#include <string>
#include "engine/rocks_wrapper.h"

namespace TKV {
class SstFileWriter {
pulic:
    SstFileWriter(const rocksdb::Options& options): _options(options) {
        _options.bottommost_compression = rocksdb::kLZ4Compression;
        _options.bottommost_compression_opts = rocksdb::CompressionOptions();
        _sst_writer.reset(new rocksdb::SstFileWriter(rocksdb::EnvOptions(), _options, nullptr, true));
    }
    rocksdb::Status open(const std::string& sst_file) {
        return _sst_writer->Open(sst_file);
    } 
    rocksdb::Status put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        return _sst_writer->Put(key, value);
    }
    rocksdb::Status finish(rocksdb::ExternalSstFileInfo* file_info = nullptr) {
        return _sst_writer->Finish(file_info);
    }
    uint64_t file_size() const {
        return _sst_writer->FileSize();
    }

    Virtual ~SstFileWriter() {}

private:
    rocksdb::Options _options;
    std::unique_ptr<rocksdb::SstFileWriter> _sst_writer = nullptr;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
