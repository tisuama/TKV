cmake_minimum_required(VERSION 3.2)
project(TKV C CXX)

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

# gtest
find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h)
find_library(GTEST_LIBRARIES NAMES gtest)
if ((NOT GTEST_INCLUDE_DIR) OR (NOT GTEST_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find gtest")
endif()

# brpc
find_path(BRPC_INCLUDE_DIR NAMES brpc/server.h)
find_library(BRPC_LIBRARIES NAMES brpc)
if ((NOT BRPC_INCLUDE_DIR) OR (NOT BRPC_LIBRARIES))
	message(FATAL_ERROR "Fail to find brpc")
endif()

# rocksdb
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
find_library(ROCKSDB_LIBRARIES NAMES rocksdb)
if ((NOT ROCKSDB_INCLUDE_DIR) OR (NOT ROCKSDB_LIBRARIES)) 
	mesage(FATAL_ERROR "Fail to find rocksdb")
endif()

# braft
find_path(BRAFT_INCLUDE_DIR NAMES braft/raft.h)
find_library(BRAFT_LIBRARIES NAMES braft)
if ((NOT BRAFT_INCLUDE_DIR) OR (NOT BRAFT_LIBRARIES)) 
	message(FATAL_ERROR "Fail to find braft")
endif()

find_package(Protobuf)
find_library(PROTOBUF_LIBRARIES NAMES protobuf)
file(GLOB PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/*.proto)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
foreach (PROTO ${PROTO_FILES})
    message(proto: ${PROTO})
    get_filename_component(PROTO_WE ${PROTO} NAME)
    string(REPLACE ".proto" "" PROTO_WE ${PROTO_WE})
    list(APPEND PROTO_HEADERS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.h")
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc")
    execute_process(COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/proto --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO})
endforeach()    
add_library(proto_src_lib OBJECT ${PROTO_SRCS})

file(GLOB COMMON_SRC
	src/common/*.cpp
	)
add_library(common_src_lib OBJECT ${COMMON_SRC})

file(GLOB META_INTERACT 
	src/meta/meta_server_interact.cpp
	)
add_library(meta_interact_lib OBJECT ${META_INTERACT})

file(GLOB STORE_INTERACT 
	src/store/store_server_interact.cpp
	)
add_library(store_interact_lib OBJECT ${STORE_INTERACT})

file(GLOB RAFT_SRC
	src/raft/*.cpp
	)
add_library(raft_src_lib OBJECT ${RAFT_SRC})

file(GLOB RAFT_META_SRC
	src/meta/raft_helper.cpp
	)
add_library(raft_meta_src_lib OBJECT ${RAFT_META_SRC})

file(GLOB META_SERVER
	src/meta/meta_server.cpp
	src/meta/common_state_machine.cpp
	src/meta/meta_state_machine.cpp
	src/meta/cluster_manager.cpp
	src/meta/meta_rocksdb.cpp
	src/meta/privilege_manager.cpp
	src/meta/schema_manager.cpp
	src/meta/namespace_manager.cpp
	src/meta/database_manager.cpp
	src/meta/table_manager.cpp
	src/meta/region_manager.cpp
	src/meta/query_region_manager.cpp
	src/meta/tso_state_machine.cpp
	)
add_library(meta_server_lib OBJECT ${META_SERVER})

file(GLOB ENGINE_SRC
	src/engine/*.cpp
	)
add_library(engine_src_lib OBJECT ${ENGINE_SRC})

file(GLOB RAFT_STORE_SRC
	src/store/raft_helper.cpp
	)
add_library(raft_store_src_lib OBJECT ${RAFT_STORE_SRC})

file(GLOB STORE_SRC
	src/store/store.cpp
	src/store/region.cpp
	src/store/region_control.cpp
	src/store/rpc_sender.cpp
	src/store/closure.cpp
	src/store/meta_writer.cpp
	src/store/closure.cpp
	)
add_library(store_src_lib OBJECT ${STORE_SRC})

file(GLOB TKV_CLIENT
	src/client/*.cpp
	)
add_library(tkv_client_src OBJECT ${TKV_CLIENT})

SET(DEP_INC
	${CMAKE_CURRENT_BINARY_DIR}/proto/
	${GFLAGS_INCLUDE_DIR}
	${GTEST_INCLUDE_DIR}
	${BRPC_INCLUDE_DIR}
	${BRAFT_INCLUDE_DIR}
	${PROTOBUF_INCLUDE_DIR}
	${ROCKSDB_INCLUDE_DIR}
)

include_directories(
	${DEP_INC}
	${CMAKE_CURRENT_BINARY_DIR}
	include/
	)

SET(DEP_LIB
	${CMAKE_THREAD_LIBS_INIT}
	${GFLAGS_LIBRARIES}
	${GTEST_LIBRARIES}
	${BRAFT_LIBRARIES}
	${BRPC_LIBRARIES}
	${PROTOBUF_LIBRARIES}
	${ROCKSDB_LIBRARIES}
	dl
	ssl
	crypto
	xxhash
)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -std=c++11 -Wall -W -fPIC -Wno-switch -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -g -std=c99 -Wall -W -fPIC -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer")

add_executable(TKVMeta
	src/meta/main.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_server_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>	
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(TKVMeta ${DEP_LIB})

add_executable(TKVStore
	src/store/main.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:raft_store_src_lib>
	)
target_link_libraries(TKVStore ${DEP_LIB})

# TKV internal test
add_executable(test_meta_server
	test/test_meta_server.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_server_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(test_meta_server ${DEP_LIB})

add_executable(test_table_key
	test/test_table_key.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_server_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(test_table_key ${DEP_LIB})

add_executable(test_log_storage
	test/test_log_storage.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(test_log_storage ${DEP_LIB})

add_executable(test_store_server
	test/test_store_server.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_server_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(test_store_server ${DEP_LIB})

add_executable(test_latch
	test/test_latch.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_server_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:store_interact_lib>
	$<TARGET_OBJECTS:store_src_lib>
	$<TARGET_OBJECTS:raft_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	$<TARGET_OBJECTS:raft_meta_src_lib>
	)
target_link_libraries(test_latch ${DEP_LIB})

add_executable(rocks_bench
	tool/rocks_bench.cpp
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:engine_src_lib>
	$<TARGET_OBJECTS:proto_src_lib>
	)
target_link_libraries(rocks_bench ${DEP_LIB})

add_library(tkv_client SHARED
	$<TARGET_OBJECTS:common_src_lib>
	$<TARGET_OBJECTS:tkv_client_src>
	$<TARGET_OBJECTS:proto_src_lib>
	$<TARGET_OBJECTS:meta_interact_lib>
	)
target_link_libraries(tkv_client ${DEP_LIB})

# TKV client test
add_executable(test_op
	test/test_op.cpp	
	)
target_link_libraries(test_op tkv_client)
