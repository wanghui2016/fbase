cmake_minimum_required(VERSION 3.5)
project(LibLRU)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

set(SOURCE_FILES src/cache.cc src/c.cpp)
add_library(lru_cache STATIC ${SOURCE_FILES})

add_executable(lru_cache_test EXCLUDE_FROM_ALL src/cache_test.cpp)
target_link_libraries(lru_cache_test lru_cache)


set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
install(FILES src/c.h DESTINATION ${PROJECT_SOURCE_DIR})
