#设置cmake的最低版本和项目名称
cmake_minimum_required(VERSION 3.10)
#项目名称
project(mprpc)

#设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

#设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
#设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
# muduo 安装路径可通过 -DMUDUO_ROOT=/path/to/muduo 覆盖（CI 用）
if(NOT MUDUO_ROOT)
    set(MUDUO_ROOT "/home/dili/local/muduo")
endif()

#设置项目编译头文件搜索路径 -I
include_directories(${PROJECT_SOURCE_DIR}/src/include)
include_directories(${PROJECT_SOURCE_DIR}/example)
include_directories(${MUDUO_ROOT}/include)
#设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)
link_directories(${MUDUO_ROOT}/lib)

#src包含mprpc框架所有相关代码
add_subdirectory(src)
#example包含mprpc框架的使用示例代码
add_subdirectory(example)
#test包含单元测试
add_subdirectory(test/unit)