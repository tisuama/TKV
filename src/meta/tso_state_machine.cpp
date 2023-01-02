#include "meta/tso_state_machine.h"
#include <fstream>

namespace TKV {
const std::string TSOStateMachine::SNAPSHOT_TSO_FILE = "tso.file";
const std::string TSOStateMachine::SNAPSHOT_TSO_FILE_WITH_SLASH = "/" + SNAPSHOT_TSO_FILE;

int TSOTimer::init(TSOStateMachine* node, int timeout_ms) {
	int ret = RepeatedTimerTask::init(timeout_ms);
	_node = node;
	return ret;
}

void TSOTimer::run() {
	_node->update_timestamp();
}

int TSOStateMachine::init(const std::vector<braft::PeerId>& peers) {
	_tso_update_timer.init(this, TSO::update_timestamp_interval_ms);
	_tso_obj.current_timestamp.set_physical(0);
	_tso_obj.current_timestamp.set_logical(0);
	_tso_obj.last_save_physical = 0;

	return CommonStateMachine::init(peers);
}

void TSOStateMachine::on_apply(braft::Iterator& iter) {
	for (; iter.valid(); iter.next()) {
		braft::Closure* done = iter.done();
		brpc::ClosureGuard done_guard(done);
		if (done) {
			((TSOClosure*)done)->raft_time_cost = ((TSOClosure*)done)->time_cost.get_time();
		}
		butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
		pb::TSORequest request;
		if (!request.ParseFromZeroCopyStream(&wrapper)) {
			DB_FATAL("Parse from zero copy input stream failed");
			if (done) {
				auto tso_done = static_cast<TSOClosure*>(done);
				if (tso_done->response) {
					tso_done->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
					tso_done->response->set_errmsg("parse from pb failed");
				}
				braft::run_closure_in_bthread(done_guard.release());
			}
			continue;
		}
		if (done && ((TSOClosure*)done)->response) {
			((TSOClosure*)done)->response->set_op_type(request.op_type());
		}

		switch(request.op_type()) {
		case pb::OP_RESET_TSO: {
			DB_FATAL("OP_RESET_TSO not impl");
			break;
		}
		case pb::OP_UPDATE_TSO: {
			update_tso(request, done);
			break;
		}
		default: 
			DB_FATAL("op_type: %d not support", request.op_type());
			IF_DONE_SET_RESPONSE(done, pb::UNSUPPORT_REQ_TYPE, "unsupport request op_type");
		}
		if (done) {
			braft::run_closure_in_bthread(done_guard.release());
		}
	}
}

void TSOStateMachine::process(google::protobuf::RpcController* controller,
			 const pb::TSORequest* request,
			 pb::TSOResponse* response,
			 google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	if (request->op_type() == pb::OP_QUERY_TSO_INFO) {
		response->set_errcode(pb::SUCCESS);
		response->set_errmsg("success");
		response->set_op_type(request->op_type());
		response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
		response->set_system_time(TSO::clock_realtime_ms());
		response->set_save_physical(_tso_obj.last_save_physical);
		auto timestamp = response->mutable_start_timestamp();
		timestamp->CopyFrom(_tso_obj.current_timestamp);
		return ;
	}

	brpc::Controller* cntl = (brpc::Controller*)controller;

	const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
	const char* remote_side = remote_side_tmp.c_str();

	if (!_is_leader) {
		response->set_errcode(pb::NOT_LEADER);
		response->set_errmsg("Not Leader");
		response->set_op_type(request->op_type());
		response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
		DB_WARNING("region_id: %ld not leader, request: %s, remote_side: %s",
				_dummy_region_id, request->ShortDebugString().c_str(), remote_side);
		return ;
	}

	// gen_tso在Raft外执行
	if (request->op_type() == pb::OP_GEN_TSO) {
		gen_tso(request, response);
		return ;
	}

	butil::IOBuf data;
	butil::IOBufAsZeroCopyOutputStream wrapper(&data);
	if (!request->SerializeToZeroCopyStream(&wrapper)) {
		cntl->SetFailed(brpc::EREQUEST, "Fail to serialze request");
		return ;
	}
	TSOClosure* c = new TSOClosure;
	c->cntl = cntl;
	c->response = response;
	c->done = done_guard.release();
	c->common_state_machine = this;

	braft::Task task;
	task.data = &data;
	task.done = c;
	_node.apply(task);
}

void TSOStateMachine::gen_tso(const pb::TSORequest* request, pb::TSOResponse* response) {
	int64_t count = response->count();
	response->set_op_type(request->op_type());
	if (count == 0) {
		response->set_errcode(pb::INPUT_PARAM_ERROR);
		response->set_errmsg("TSO count should be positive");
		return ;
	}
	if (!_is_health) {
		DB_FATAL("TSO has wrong status, retry later");
		response->set_errcode(pb::RETRY_LATER);
		response->set_errmsg("TSO not ready, retry later");
		return ;
	}

	pb::TSOTimestamp current;
	bool need_retry = false;
	for (size_t i = 0; i < 50; i++) {
		BAIDU_SCOPED_LOCK(_tso_mutex);
		int64_t physical = _tso_obj.current_timestamp.physical();
		if (physical != 0) {
			int64_t new_logical = _tso_obj.current_timestamp.logical() + count;
			if (new_logical < TSO::max_logical) {
				current.CopyFrom(_tso_obj.current_timestamp);
				_tso_obj.current_timestamp.set_logical(new_logical);
				need_retry = false;
			} else {
				DB_WARNING("TSO logical part outside of max logical interval, retry later, check ntp time");
				need_retry = true;
			}
		} else {
			DB_WARNING("TSO not ready, physical = 0, retry later");
			need_retry = true;
		}
	}
	if (need_retry) {
		response->set_errcode(pb::EXEC_FAIL);
		response->set_errmsg("TSO gen tso failed");
		DB_FATAL("TSO gen tso failed");
		return ;
	}

	auto timestamp = response->mutable_start_timestamp();
	timestamp->CopyFrom(current);
	response->set_count(count);
	response->set_errcode(pb::SUCCESS);
}

void TSOStateMachine::update_tso(const pb::TSORequest& request, braft::Closure* done) {
	int64_t physical = request.save_physical();
	pb::TSOTimestamp current = request.current_timestamp();
	// 不能回退
	if (physical < _tso_obj.last_save_physical || 
	    current.physical() < _tso_obj.current_timestamp.physical()) {
		DB_FATAL("TSO Time fallback save_physical: (%ld, %ld), physical: (%ld, %ld), logical: (%ld, %ld)",
			physical, _tso_obj.last_save_physical, current.physical(), _tso_obj.current_timestamp.physical(), 
			current.logical(), _tso_obj.current_timestamp.logical());	
		if (done && ((TSOClosure*)done)->response) {
			pb::TSOResponse* response = ((TSOClosure*)done)->response;
			response->set_errcode(pb::INTERNAL_ERROR);
			response->set_errmsg("Time can't fallback");
		}
		return ;
	}
	{
		BAIDU_SCOPED_LOCK(_tso_mutex);
		_tso_obj.last_save_physical = physical;
		_tso_obj.current_timestamp.CopyFrom(current);
	}
	if (done && ((TSOClosure*)done)->response) {
		pb::TSOResponse* response = ((TSOClosure*)done)->response;
		response->set_errcode(pb::SUCCESS);
		response->set_errmsg("success");
	}
}

int TSOStateMachine::load_tso(const std::string& tso_file) {
	std::ifstream extra_fs(tso_file);
	std::string extra((std::istreambuf_iterator<char>(extra_fs)),   
			std::istreambuf_iterator<char>());
	_tso_obj.last_save_physical = std::stol(extra);
	return 0;
}

int TSOStateMachine::sync_timestamp(const pb::TSOTimestamp& current_timestamp, int64_t save_physical) {
	pb::TSORequest  request;
	pb::TSOResponse response;
	request.set_op_type(pb::OP_UPDATE_TSO);
	auto timestamp = request.mutable_current_timestamp();
	timestamp->CopyFrom(current_timestamp);
	request.set_save_physical(save_physical);

	butil::IOBuf data;
	butil::IOBufAsZeroCopyOutputStream wrapper(&data);
	if (!request.SerializeToZeroCopyStream(&wrapper)) {
		DB_WARNING("Fail to serialze request");
		return -1;
	}

	BthreadCond sync_cond;
	TSOClosure* c = new TSOClosure(&sync_cond);
	c->response = &response;
	c->done = nullptr;
	c->common_state_machine = this;
	sync_cond.increase();

	braft::Task task;
	task.data = &data;
	task.done = c;
	_node.apply(task);
	sync_cond.wait();

	if (response.errcode() != pb::SUCCESS) {
		DB_FATAL("TSO region_id: %ld sync timestamp failed, response: %s",
				_dummy_region_id, response.ShortDebugString().c_str());
		return -1;
	}
	return 0;
}

void TSOStateMachine::update_timestamp() {
	// 定时任务触发
	if (!_is_leader) {
		return ;
	}
	int64_t now = TSO::clock_realtime_ms();
	int64_t prev_physical = 0;
	int64_t prev_logical = 0;
	int64_t last_save = 0;
	{
		BAIDU_SCOPED_LOCK(_tso_mutex);
		prev_physical = _tso_obj.current_timestamp.physical();
		prev_logical =  _tso_obj.current_timestamp.logical();
		last_save    =  _tso_obj.last_save_physical;
	}
	int64_t delta = now - prev_physical;
	if (delta < 0) {
		DB_WARNING("physical time is slow, now: %ld, prev: %ld", now, prev_physical);
		return ;
	}

	int64_t next = now;
	if (delta > TSO::update_timestamp_guard_ms) {
		next = now;
	} else if (prev_logical > TSO::max_logical / 2) {
		// 当请求tso数量过多的时候，需要稍微提升下物理时间
		next = now + TSO::update_timestamp_guard_ms;
	} else {
		DB_WARNING("TSO no need to update timestamp, prev: %ld, now: %ld, save: %ld",
				prev_physical, now, last_save);
	}
	int save = last_save;
	if (save - next <= TSO::update_timestamp_guard_ms) {
		save = next + TSO::save_interval_ms;
	}
	pb::TSOTimestamp tp;
	tp.set_physical(next);
	tp.set_logical(0);
	// 走Raft 
	sync_timestamp(tp, save);
}

void TSOStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
	DB_WARNING("region_id: %ld start on snapshot save", _dummy_region_id);

	std::string tso_str = std::to_string(_tso_obj.last_save_physical);
	Bthread bth;
	std::function<void()> save_snapshot_fn = [this, done, writer, tso_str]() {
		save_snapshot(done, writer, tso_str);
	};
	bth.run(save_snapshot_fn);
}

void TSOStateMachine::save_snapshot(braft::Closure* done, braft::SnapshotWriter* writer, std::string tso_str) {
	brpc::ClosureGuard done_guard(done);
	std::string snapshot_path = writer->get_path();
	std::string save_path = snapshot_path + SNAPSHOT_TSO_FILE_WITH_SLASH;
	std::ofstream extra_fs(save_path, std::ofstream::out | std::ofstream::trunc);
	extra_fs.write(tso_str.data(), tso_str.size());
	extra_fs.close();
	if (writer->add_file(SNAPSHOT_TSO_FILE_WITH_SLASH) != 0) {
		done->status().set_error(EINVAL, "Fail to add file");
		DB_FATAL("region_id: %ld Error while adding file to writer", _dummy_region_id);
		return ;
	}
	DB_WARNING("region_id: %ld save physical string: %s when snapshot", tso_str.c_str());
}

int TSOStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
	DB_WARNING("region_id: %ld start on snapshot load", _dummy_region_id);
	std::vector<std::string> files;
	reader->list_files(&files);
	for (auto file: files) {
		DB_WARNING("region_id: %ld snapshot load file: %s", file.c_str());
		if (file == SNAPSHOT_TSO_FILE_WITH_SLASH) {
			std::string tso_file = reader->get_path() + SNAPSHOT_TSO_FILE_WITH_SLASH;
			if (load_tso(tso_file)) {
				DB_WARNING("region_id: %ld load tso file failed", _dummy_region_id);
				return -1;
			}
		}
	}
	set_have_data(true);
	return 0;
}

void TSOStateMachine::on_leader_start() {
	DB_WARNING("region_id: %ld tso leader start", _dummy_region_id);
	int64_t now = TSO::clock_realtime_ms();
	pb::TSOTimestamp current;
	current.set_physical(now);
	current.set_logical(0);

	int64_t last_save = _tso_obj.last_save_physical;
	// gen_tso时不走raft，update_timestamp_interval_ms时间内的时间戳可能已经被使用了
	// 这里判断now这个时间戳是不是够新
	if (last_save - now < TSO::update_timestamp_interval_ms) {
		current.set_physical(last_save + TSO::update_timestamp_guard_ms);
		last_save = now + TSO::save_interval_ms;
	}

	auto fn = [this, last_save, current]() {
		DB_WARNING("region_id: %ld leader start, save physical: %ld, current: (%ld, %ld)", 
				_dummy_region_id, last_save, current.physical(), current.logical());
		int ret = sync_timestamp(current, last_save);
		if (ret < 0) {
			_is_health = false;
		}
		DB_WARNING("region_id: %ld sync timestamp success", _dummy_region_id);
		_is_leader.store(true);
		_tso_update_timer.start();
	};
	Bthread bth;
	bth.run(fn);
}

void TSOStateMachine::on_leader_stop() {
	_tso_update_timer.stop();
	DB_WARNING("region_id: %ld leader stop", _dummy_region_id);
	CommonStateMachine::on_leader_stop();
}

} // namespace TKV
