set(SRC_LIST main.cc userservice.cc friendservice.cc taskservice.cc ../user.pb.cc ../friend.pb.cc ../task.pb.cc)
add_executable(provider ${SRC_LIST})
target_link_libraries(provider mprpc protobuf)
