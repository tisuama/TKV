#include "txn/txn.h"

namespace TKV {
void Txn::add_mutation(const pb::Mutation& m) {
    _mutations.push_back(m);
    _keys.push_back(m.key());
}

void Txn::add_secondary(const std::string& key) {
    // async commit + primary row才回出现
    _secondaries.push_back(key);
}

void Txn::pwrite(const pb::Mutation& m, const std::string& primary) {
}

void Txn::commit(const std::string& key, uint64_t commit_ts) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
