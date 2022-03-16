cmake_minimum_required(VERSION 3.2)
project(TVK C CXX)

option(DEBUG "print debug log" ON)
option(WITH_TESTS "With tests" ON)

message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_COMPILER_VERSION}")

include(FindThreads)
include(FindProtobuf)

find_package(OpenSSL REQUIRED)

# gflags
find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h)
find_library(GFLAGS_LIBRARIES NAMES gflags)
if ((NOT GFLAGS_INCLUDE_DIR) OR (NOT GFLAGS_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find glog")
endif()

# glog(BRPC_WITH_GLOG)
# find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h)
# find_library(GLOG_LIBRARIES NAMES glog)
# if ((NOT GLOG_INCLUDE_DIR) OR (NOT GLOG_LIBRARIES)) 
# 	message(FATAL_ERROR "Fail to find glog")
# endif()

# brpc
find_path(BRPC_INCLUDE_DIR NAMES brpc/server.h)
find_library(BRPC_LIBRARIES NAMES brpc)
if ((NOT BRPC_INCLUDE_DIR) OR (NOT BRPC_LIBRARIES))
	message(FATAL_ERROR "Fail to find brpc")
endif()

# braft
find_path(BRAFT_INCLUDE_DIR NAMES braft/raft.h)
find_library(BRAFT_LIBRARIES NAMES braft)
if ((NOT BRAFT_INCLUDE_DIR) OR (NOT BRAFT_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find braft")
endif()

file(GLOB COMMON_SRC
	src/common/*.cpp
	)
add_library(common_src_lib OBJECT ${COMMON_SRC})

SET(DEP_INC
	${CMAKE_CURRENT_BINARY_DIR}/proto
	${GFLAGS_INCLUDE_DIR}
	${BRPC_INCLUDE_DIR}
	${BRAFT_INCLUDE_DIR}
)

include_directories(
	${DEP_INC}
	include/
	)

SET(DEP_LIB
	${CMAKE_THREAD_LIBS_INIT}
	${GFLAGS_LIBRARIES}
	${BRAFT_LIBRARIES}
	${BRPC_LIBRARIES}
	dl
	ssl
	crypto
)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Wall -W -fPIC -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -Wall -W -fPIC -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer")
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(TKVMeta
	src/meta/main.cpp
	$<TARGET_OBJECTS:common_src_lib>
	)
target_link_libraries(TKVMeta ${DEP_LIB})
