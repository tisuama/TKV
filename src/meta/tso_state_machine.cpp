#include "meta/tso_state_machine.h"

namespace TKV {
const std::string TSOStateMachine::SNAPSHOT_TSO_FILE = "tso.file";
const std::string TSOStateMachine::SNAPSHOT_TSO_FILE_WITH_SLASH = "/" + SNAPSHOT_TSO_FILE;

int TSOTimer:init(TSOStateMachine* node, int timeout_ms) {
	int ret = RepeatedTimerTask::init(timeout_ms);
	_node = node;
	return ret;
}

void TSOTimer::run() {
	_node->update_timestamp();
}

int  TSOStateMachine::init(const std::vector<braft::PeerId>& peers) {
}

void TSOStateMachine::on_apply(braft::Iterator& iter);

void TSOStateMachine::process(google::protobuf::RpcController* controller,
			 const pb::TSORequest* request,
			 pb::TSOResponse* response,
			 google::protobuf::Closure* done);

void TSOStateMachine::gen_tso(const pb::TSORequest* request, pb::TSOResponse* response);

void TSOStateMachine::reset_tso(const pb::TSORequest& request, braft::Closure* done);

void TSOStateMachine::update_tso(const pb::TSORequest& request, braft::Closure* done);

int TSOStateMachine::load_tso(const std::string& tso_file);

int TSOStateMachine::sync_timestamp(const pb::TSOTimestamp& current_timestamp, int64_t save_physical);

void TSOStateMachine::update_timestamp();

void TSOStateMachine::save_snapshot(braft::Closure* done, braft::SnapshotWriter* writer, std::stirng& tso_str);

int TSOStateMachine::on_snapshot_load(braft::SnapshotReader* reader);

void TSOStateMachine::on_snapshot_start();

void TSOStateMachine::on_snapshot_stop();

} // namespace TKV
