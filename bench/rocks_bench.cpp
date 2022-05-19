#include "common/common.h"
#include "engine/rocks_wrapper.h"
#include <random>    
// Prints logs to stderr for faster debugging
class StderrLogger : public rocksdb::Logger {
 public:
  explicit StderrLogger(const rocksdb::InfoLogLevel log_level = rocksdb::InfoLogLevel::INFO_LEVEL)
      : Logger(log_level) {}

  // Brings overloaded Logv()s into scope so they're not hidden when we override
  // a subset of them.
  using rocksdb::Logger::Logv;

  virtual void Logv(const char* format, va_list ap) override {
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
  }
};
std::string gen_random(const int seed, const int len){
    static std::mt19937 mt(seed);
    static std::uniform_int_distribution<int> dist(32, 93);
	std::string str;
    for (int i = 0; i < len; ++i) {
		str += dist(mt);
    }
	return str;
}

static void single_put(int i) {
	rocksdb::WriteBatch batch;
	auto instance = TKV::RocksWrapper::get_instance();
	auto db = instance->get_db();
	auto data_cf = instance->get_data_handle();
	auto option = rocksdb::WriteOptions();
	option.disableWAL = true;
	std::string key = gen_random(i, 30);
	std::string value = gen_random(i, 300);
	batch.Put(data_cf, key, value);
	auto s = db->Write(option, &batch); 
	assert(s.ok());
}

int main() {
	auto db = TKV::RocksWrapper::get_instance();
	db->init("./test_data");
	for (int i = 0; i < 1000000; i++) {
		if (i % 100000 == 0) {
			fprintf(stderr, "=== round %d\n", i / 100000);
		}	
		TKV::Bthread bth;
		auto f = [&i]() {
			single_put(i);	
		};		
		bth.run(f);
		bth.join();
	}	
}
