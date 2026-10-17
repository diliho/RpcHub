# 显式列出所有源文件
set(SRC_LIST
    Logger.cc
    mprpcapplication.cc
    mprpcchannel.cc
    mprpcconfig.cc
    mprpccontroller.cc
    mprpcprovider.cc
    rpcerror.cc
    rpcheader.pb.cc
    zookeeperutil.cc
    redisutil.cc
    ratelimiter.cc
    metricscollector.cc
)

# 添加头文件搜索路径
include_directories(./include)
# 添加muduo库的头文件搜索路径（继承根 CMakeLists 的 MUDUO_ROOT）
include_directories(${MUDUO_ROOT}/include)
link_directories(${MUDUO_ROOT}/lib)

# 将共享库改为静态库
add_library(mprpc STATIC ${SRC_LIST})

link_libraries(muduo_net muduo_base pthread hiredis)
target_link_libraries(mprpc muduo_net muduo_base pthread zookeeper_mt hiredis)