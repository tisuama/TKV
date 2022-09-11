#pragma once
#include <proto/raft.pb.h>
#include <braft/raft.h>

namespace TKV {

extern void common_raft_control(google::protobuf::RpcController* controller,
        const pb::RaftControlRequest* request,
        pb::RaftControlResponse* response,
        google::protobuf::Closure* done,
        braft::Node* node);
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
