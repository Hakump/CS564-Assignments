cmake_minimum_required(VERSION 3.10)
project(code)

#set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")

add_executable(code wl.cpp)