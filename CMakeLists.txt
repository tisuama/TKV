cmake_minimum_required(VERSION 3.2)
project(TVK C CXX)

option(DEBUG "print debug log" ON)
option(WITH_TESTS "With tests" ON)

message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_COMPILER_VERSION}")

include (FindThreads)

find_package(OpenSSL REQUIRED)
ADD_LIBRARY(ssl SHARED IMPORTED GLOBAL)
ADD_LIBRARY(crypto SHARED IMPORTED GLOBAL)

# glog
find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h)
find_library(GLOG_LIBRARIES NAMES glog)
if ((NOT GLOG_INCLUDE_DIR) OR (NOT GLOG_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find glog")
endif()
ADD_LIBRARY(glog SHARED IMPORTED GLOBAL)

# brpc
find_path(BRPC_INCLUDE_DIR NAMES brpc/server.h)
find_library(BRPC_LIBRARIES NAMES brpc)
if ((NOT BRPC_INCLUDE_DIR) OR (NOT BRPC_LIBRARIES))
	message(FATAL_ERROR "Fail to find brpc")
endif()

#braft
find_path(BRAFT_INCLUDE_DIR NAMES braft/raft.h)
find_library(BRAFT_LIBRARIES NAMES braft)
if ((NOT BRAFT_INCLUDE_DIR) OR (NOT BRAFT_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find braft")
endif()

SET(DEP_INC
	${CMAKE_CURRENT_BINARY_DIR}/proto
)

SET(DEP_LIB
	${BRAFT_LIBRARIES}
	${BRPC_LIBRARIES}
)

SET(DEP_LIB
	${DEP_LIB}
	ssl
)
