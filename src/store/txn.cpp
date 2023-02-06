#include "store/txn.h"

namespace TKV {
void Txn::pwrite(const pb::Mutation& m, const std::string& primary) {
}

void Txn::commit(const std::string& key, uint64_t commit_ts) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
