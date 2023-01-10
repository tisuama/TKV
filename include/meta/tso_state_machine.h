#pragma once
#include <time.h>
#include <braft/repeated_timer_task.h>
#include "meta/common_state_machine.h"

namespace TKV {
class TSOStateMachine;
class TSOTimer: public braft::RepeatedTimerTask {
public:
	TSOTimer()
		: _node(nullptr) 
	{}

	virtual ~TSOTimer() {}

	int init(TSOStateMachine* node, int timeout_ms);
	virtual void run();

protected:
	virtual void on_destroy() {}
	TSOStateMachine* _node;
};

struct TSOObj {
	pb::TSOTimestamp current_timestamp;
	int64_t 		 last_save_physical;
};

class TSOStateMachine: public TKV::CommonStateMachine {
public:
	TSOStateMachine(const braft::PeerId& peer_id)
		: CommonStateMachine(1 /* region_id */, 
				"tso_raft" /* identify */, 
				"/tso" /* file_path */, 
				peer_id)
	{
		bthread_mutex_init(&_tso_mutex, nullptr);
	}

	virtual ~TSOStateMachine() {
		_tso_update_timer.stop();
		_tso_update_timer.destroy();
		bthread_mutex_destroy(&_tso_mutex);
	}

	virtual int init(const std::vector<braft::PeerId>& peers);

	virtual void on_apply(braft::Iterator& iter);

	void process(google::protobuf::RpcController* controller,
				 const pb::TSORequest* request,
				 pb::TSOResponse* response,
				 google::protobuf::Closure* done);

	void gen_tso(const pb::TSORequest* request, pb::TSOResponse* response);

	void update_tso(const pb::TSORequest& request, braft::Closure* done);

	int load_tso(const std::string& tso_file);

	int sync_timestamp(const pb::TSOTimestamp& current_timestamp, int64_t save_physical);

	void update_timestamp();

	virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

	void save_snapshot(braft::Closure* done, braft::SnapshotWriter* writer, std::string tso_str);

	virtual int on_snapshot_load(braft::SnapshotReader* reader);

	virtual void on_leader_start();

	virtual void on_leader_stop();

	static const std::string SNAPSHOT_TSO_FILE;
	static const std::string SNAPSHOT_TSO_FILE_WITH_SLASH;

private:
	TSOTimer 	_tso_update_timer;
	TSOObj      _tso;
	bthread_mutex_t 	_tso_mutex;
	bool 				_is_health {true};
};
	
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
