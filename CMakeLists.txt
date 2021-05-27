cmake_minimum_required(VERSION 3.10)
project(kvrocks
        VERSION 2.0.1
        DESCRIPTION "NoSQL which based on rocksdb and compatible with the Redis protocol"
        LANGUAGES CXX)

# External Dependences
include(ExternalProject)

set(default_build_type "Release")
if (NOT CMAKE_BUILD_DIRECTORY)
    set(CMAKE_BUILD_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
# GLIBC < 2.17 should explict specify the real time library when use clock_*
find_library(REALTIME_LIB rt)
if (REALTIME_LIB)
    list(APPEND EXTERNAL_LIBS PRIVATE rt)
endif()

include(cmake/jemalloc.cmake)
include(cmake/glog.cmake)
include(cmake/snappy.cmake)
include(cmake/rocksdb.cmake)
include(cmake/libevent.cmake)

list(APPEND EXTERNAL_LIBS PRIVATE ${jemalloc_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${jemalloc_INCLUDE_DIRS})

list(APPEND EXTERNAL_LIBS PRIVATE ${glog_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${glog_INCLUDE_DIRS})

list(APPEND EXTERNAL_LIBS PRIVATE ${rocksdb_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${rocksdb_INCLUDE_DIRS})

list(APPEND EXTERNAL_LIBS PRIVATE ${snappy_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${snappy_INCLUDE_DIRS})

list(APPEND EXTERNAL_LIBS PRIVATE ${libevent_LIBRARIES})
list(APPEND EXTERNAL_INCS PRIVATE ${libevent_INCLUDE_DIRS})

# End dependences

# Add git sha to version.h
find_package(Git REQUIRED)
execute_process(COMMAND git rev-parse --short HEAD OUTPUT_VARIABLE GIT_SHA)
string(STRIP ${GIT_SHA} GIT_SHA)
configure_file(src/version.h.in ${PROJECT_BINARY_DIR}/version.h)

# Main target
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
endif()
add_executable(kvrocks )
target_compile_features(kvrocks PRIVATE cxx_std_11)
target_compile_options(kvrocks PRIVATE -Wall -Wpedantic -g -Wsign-compare -Wreturn-type -fno-omit-frame-pointer -O0)
option(ENABLE_ASAN "enable ASAN santinizer" OFF)
if(ENBALE_ASAN)
    target_compile_options(kvrocks PRIVATE -fsanitize=address)
    target_link_libraries(kvrocks PRIVATE -fsanitize=address)
endif()
add_dependencies(kvrocks jemalloc libevent glog snappy rocksdb)
target_include_directories(kvrocks PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocks ${EXTERNAL_INCS})
find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(kvrocks PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(kvrocks PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif()
target_link_libraries(kvrocks PRIVATE -fno-omit-frame-pointer)
target_link_libraries(kvrocks ${EXTERNAL_LIBS})
target_sources(kvrocks PRIVATE
        src/redis_db.cc
        src/redis_db.h
        src/redis_connection.cc
        src/redis_connection.h
        src/compact_filter.cc
        src/compact_filter.h
        src/worker.cc
        src/worker.h
        src/main.cc
        src/redis_request.cc
        src/redis_request.h
        src/redis_cmd.cc
        src/redis_cmd.h
        src/util.cc
        src/util.h
        src/geohash.cc
        src/geohash.h
        src/storage.cc
        src/storage.h
        src/status.h
        src/redis_reply.h
        src/redis_reply.cc
        src/replication.cc
        src/replication.h
        src/task_runner.cc
        src/task_runner.h
        src/encoding.h
        src/encoding.cc
        src/redis_metadata.h
        src/redis_metadata.cc
        src/redis_string.h
        src/redis_string.cc
        src/redis_hash.h
        src/redis_hash.cc
        src/redis_list.h
        src/redis_list.cc
        src/redis_set.h
        src/redis_set.cc
        src/redis_zset.cc
        src/redis_zset.h
        src/redis_geo.cc
        src/redis_geo.h
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/redis_bitmap_string.cc
        src/redis_bitmap_string.h
        src/redis_pubsub.cc
        src/redis_pubsub.h
        src/redis_sortedint.cc
        src/redis_sortedint.h
        src/lock_manager.cc
        src/rocksdb_crc32c.h
        src/config.cc
        src/config.h
        src/config_type.h
        src/stats.cc
        src/stats.h
        src/server.cc
        src/server.h
        src/cron.cc
        src/cron.h
        src/event_listener.h
        src/event_listener.cc
        src/log_collector.h
        src/log_collector.cc
        src/table_properties_collector.cc
        src/table_properties_collector.h
        src/compaction_checker.cc
        src/compaction_checker.h
        )

# kvrocks2redis sync tool
add_executable(kvrocks2redis)
target_compile_features(kvrocks2redis PRIVATE cxx_std_11)
target_compile_options(kvrocks2redis PRIVATE -Wall -Wpedantic -g -Wsign-compare -Wreturn-type)
option(ENABLE_ASAN "enable ASAN santinizer" OFF)
if(ENBALE_ASAN)
    target_compile_options(kvrocks2redis PRIVATE -fno-omit-frame-pointer -fsanitize=address)
    target_link_libraries(kvrocks2redis PRIVATE -fno-omit-frame-pointer -fsanitize=address)
endif()
add_dependencies(kvrocks2redis libevent glog rocksdb)
target_include_directories(kvrocks2redis PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocks2redis ${EXTERNAL_INCS})

find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(kvrocks2redis PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(kvrocks2redis PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif()
target_link_libraries(kvrocks2redis ${EXTERNAL_LIBS})
target_sources(kvrocks2redis PRIVATE
        src/redis_db.cc
        src/redis_db.h
        src/compact_filter.cc
        src/compact_filter.h
        src/worker.cc
        src/worker.h
        src/util.cc
        src/util.h
        src/geohash.cc
        src/geohash.h
        src/redis_connection.cc
        src/redis_connection.h
        src/redis_request.cc
        src/redis_request.h
        src/redis_cmd.cc
        src/redis_cmd.h
        src/storage.cc
        src/storage.h
        src/status.h
        src/redis_reply.h
        src/redis_reply.cc
        src/task_runner.cc
        src/task_runner.h
        src/encoding.h
        src/encoding.cc
        src/redis_metadata.h
        src/redis_metadata.cc
        src/redis_string.h
        src/redis_string.cc
        src/redis_hash.h
        src/redis_hash.cc
        src/redis_list.h
        src/redis_list.cc
        src/redis_set.h
        src/redis_set.cc
        src/redis_zset.cc
        src/redis_zset.h
        src/redis_geo.cc
        src/redis_geo.h
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/redis_bitmap_string.cc
        src/redis_bitmap_string.h
        src/redis_pubsub.cc
        src/redis_pubsub.h
        src/redis_sortedint.cc
        src/redis_sortedint.h
        src/replication.cc
        src/replication.h
        src/lock_manager.cc
        src/rocksdb_crc32c.h
        src/config.cc
        src/config.h
        src/config_type.h
        src/stats.cc
        src/stats.h
        src/server.cc
        src/server.h
        src/cron.cc
        src/cron.h
        src/event_listener.h
        src/event_listener.cc
        src/log_collector.h
        src/log_collector.cc
        src/table_properties_collector.cc
        src/table_properties_collector.h
        src/compaction_checker.cc
        src/compaction_checker.h
        tools/kvrocks2redis/config.cc
        tools/kvrocks2redis/config.h
        tools/kvrocks2redis/main.cc
        tools/kvrocks2redis/sync.cc
        tools/kvrocks2redis/sync.h
        tools/kvrocks2redis/util.cc
        tools/kvrocks2redis/util.h
        tools/kvrocks2redis/redis_writer.cc
        tools/kvrocks2redis/redis_writer.h
        tools/kvrocks2redis/writer.cc
        tools/kvrocks2redis/writer.h
        tools/kvrocks2redis/parser.cc
        tools/kvrocks2redis/parser.h)

# kvrocksrestore restore backup tool
add_executable(kvrocksrestore)
target_compile_features(kvrocksrestore PRIVATE cxx_std_11)
target_compile_options(kvrocksrestore PRIVATE -Wall -Wpedantic -g -Wsign-compare -Wreturn-type)
option(ENABLE_ASAN "enable ASAN santinizer" OFF)
if(ENBALE_ASAN)
    target_compile_options(kvrocksrestore PRIVATE -fno-omit-frame-pointer -fsanitize=address)
    target_link_libraries(kvrocksrestore PRIVATE -fno-omit-frame-pointer -fsanitize=address)
endif()
add_dependencies(kvrocksrestore glog rocksdb)
target_include_directories(kvrocksrestore PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(kvrocksrestore ${EXTERNAL_INCS})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
if(CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(kvrocksrestore PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif()
target_link_libraries(kvrocksrestore ${EXTERNAL_LIBS})
target_sources(kvrocksrestore PRIVATE
        tools/kvrocksrestore/main.cc)

add_executable(unittest
        src/server.cc
        src/server.h
        src/config.cc
        src/config.h
        src/worker.cc
        src/worker.h
        src/redis_connection.cc
        src/redis_connection.h
        src/redis_pubsub.cc
        src/redis_pubsub.h
        src/redis_cmd.cc
        src/redis_cmd.h
        src/redis_request.cc
        src/redis_request.h
        src/replication.cc
        src/replication.h
        src/redis_reply.cc
        src/redis_reply.h
        src/redis_bitmap.cc
        src/redis_bitmap.h
        src/redis_bitmap_string.cc
        src/redis_bitmap_string.h
        src/redis_metadata.cc
        src/encoding.cc
        src/redis_string.cc
        src/redis_hash.cc
        src/redis_list.cc
        src/redis_set.cc
        src/redis_zset.cc
        src/redis_geo.cc
        src/redis_sortedint.cc
        src/redis_sortedint.h
        src/util.cc
        src/geohash.cc
        src/storage.cc
        src/lock_manager.cc
        src/stats.cc
        src/event_listener.cc
        src/task_runner.cc
        src/cron.cc
        src/compact_filter.cc
        src/redis_db.cc
        src/redis_db.h
        src/log_collector.h
        src/log_collector.cc
        src/table_properties_collector.cc
        src/table_properties_collector.h
        src/compaction_checker.cc
        src/compaction_checker.h
        tests/main.cc
        tests/test_base.h
        tests/t_string_test.cc
        tests/t_encoding_test.cc
        tests/t_list_test.cc
        tests/t_hash_test.cc
        tests/t_set_test.cc
        tests/t_sortedint_test.cc
        tests/t_zset_test.cc
        tests/t_geo_test.cc
        tests/t_metadata_test.cc
        tests/t_observer_test.cc
        tests/t_semisync_master_test.cc
        tests/string_reply_test.cc
        tests/string_util_test.cc
        tests/rwlock_test.cc
        tests/cron_test.cc
        tests/config_test.cc
        tests/task_runner_test.cc
        tests/t_bitmap_test.cc
        tests/compact_test.cc
        tests/log_collector_test.cc src/config_type.h)

add_dependencies(unittest glog rocksdb snappy jemalloc)
target_compile_features(unittest PRIVATE cxx_std_11)
message(${snappy_LIBRARIES})
target_link_libraries(unittest PRIVATE ${EXTERNAL_LIBS} -lgtest)
target_include_directories(unittest PRIVATE ${PROJECT_BINARY_DIR})
target_include_directories(unittest ${EXTERNAL_INCS})
target_include_directories(unittest PRIVATE src)
