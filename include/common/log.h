// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <stdarg.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#if BRPC_WITH_GLOG
#include <glog/logging.h>
#else
#include <butil/logging.h>
#endif

namespace logging {
DECLARE_int32(v); // Used when use VLOG type
// "Any log at or above this level will be "
// "displayed. Anything below this level will be silently ignored. "
// "0=INFO 1=NOTICE 2=WARNING 3=ERROR 4=FATAL"
DECLARE_int32(minloglevel); // Default 0
}

namespace TKV {
DECLARE_string(log_path);
DECLARE_bool(enable_debug);
DECLARE_bool(enable_self_trace);

using namespace logging;
const int MAX_LOG_LEN = 2048;
inline void glog_info_writelog(const char* file, int line, const char* fmt, ...) {
    char buf[MAX_LOG_LEN];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    LOG_AT_STREAM(INFO, file, line) << buf;
}
const int MAX_LOG_LEN_LONG = 20480;
inline void glog_info_writelog_long(const char* file, int line, const char* fmt, ...) {
    char buf[MAX_LOG_LEN_LONG];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    LOG_AT_STREAM(INFO, file, line) << buf;
}
#define DB_NOTICE_LONG(_fmt_, args...) \
    do {\
        ::TKV::glog_info_writelog_long(__FILE__, __LINE__, "[%s][%llu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);

inline void glog_warning_writelog(const char* file, int line, const char* fmt, ...) {
    char buf[MAX_LOG_LEN];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    LOG_AT_STREAM(WARNING, file, line) << buf;
}
inline void glog_error_writelog(const char* file, int line, const char* fmt, ...) {
    char buf[MAX_LOG_LEN];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    LOG_AT_STREAM(ERROR, file, line) << buf;
}

#ifndef NDEBUG
#define DB_DEBUG(_fmt_, args...) \
    do {\
        if (!TKV::FLAGS_enable_debug) break; \
        ::TKV::glog_info_writelog(__FILE__, __LINE__, "[%s][%lu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);
#else
#define DB_DEBUG(_fmt_, args...)
#endif

#define DB_TRACE(_fmt_, args...) \
    do {\
        if (!TKV::FLAGS_enable_self_trace) break; \
        ::TKV::glog_info_writelog(__FILE__, __LINE__, "[%s][%lu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_NOTICE(_fmt_, args...) \
    do {\
        ::TKV::glog_info_writelog(__FILE__, __LINE__, "[%s][%lu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_WARNING(_fmt_, args...) \
    do {\
        ::TKV::glog_warning_writelog(__FILE__, __LINE__, "[%s][%lu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_FATAL(_fmt_, args...) \
    do {\
        ::TKV::glog_error_writelog(__FILE__, __LINE__, "[%s][%lu]" _fmt_, \
                __FUNCTION__, bthread_self(), ##args);\
    } while (0);

inline int init_log(const char* bin_name) {
    int ret = 0;
#if !BRPC_WITH_GLOG
	logging::LoggingSettings log_setting;
	log_setting.logging_dest = logging::LOG_TO_FILE;
    std::string log_file = FLAGS_log_path + bin_name + ".log";
	log_setting.log_file = log_file.data();
	ret = logging::InitLogging(log_setting);
#endif
    return ret ? 0: -1;
}

} //namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
