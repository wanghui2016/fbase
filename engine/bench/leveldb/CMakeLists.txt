cmake_minimum_required(VERSION 3.5)
project(leveldb_bench)

set(CMAKE_CXX_STANDARD 98)

set(SOURCE_FILES main.cpp)
add_executable(leveldb_bench ${SOURCE_FILES})
target_link_libraries(leveldb_bench leveldb glog gflags pthread)