#!/bin/bash

set -e

PROJECT_DIR=$(pwd)

# ── 1. 检查系统依赖 ──
echo ">>> Checking system dependencies..."
DEPS="cmake build-essential libboost-all-dev protobuf-compiler libprotobuf-dev libzookeeper-mt-dev libhiredis-dev"
for pkg in cmake build-essential libboost-all-dev protobuf-compiler libprotobuf-dev libzookeeper-mt-dev libhiredis-dev; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        MISSING="$MISSING $pkg"
    fi
done
if [ -n "$MISSING" ]; then
    echo ">>> Installing missing dependencies:$MISSING"
    sudo apt-get update
    sudo apt-get install -y $MISSING
fi

# ── 2. 检查并安装 muduo 库 ──
MUDUO_DIR=/home/dili/local/muduo
if [ ! -d "$MUDUO_DIR/include/muduo" ]; then
    echo ">>> Building and installing muduo library..."
    rm -rf /tmp/muduo
    git clone --depth 1 https://github.com/chenshuo/muduo.git /tmp/muduo
    mkdir -p /tmp/muduo/build
    cmake -S /tmp/muduo -B /tmp/muduo/build \
        -DCMAKE_INSTALL_PREFIX=$MUDUO_DIR \
        -DMUDUO_BUILD_EXAMPLES=OFF
    cmake --build /tmp/muduo/build -j$(nproc)
    cmake --install /tmp/muduo/build --prefix $MUDUO_DIR
    rm -rf /tmp/muduo
    echo ">>> muduo installed to $MUDUO_DIR"
else
    echo ">>> muduo already installed at $MUDUO_DIR"
fi

# ── 3. 生成 Protobuf 文件 ──
echo ">>> Regenerating protobuf files..."
# RPC 协议头
protoc --proto_path=$PROJECT_DIR/src --cpp_out=$PROJECT_DIR/src/include $PROJECT_DIR/src/rpcheader.proto
cp $PROJECT_DIR/src/include/rpcheader.pb.cc $PROJECT_DIR/src/rpcheader.pb.cc
# 示例服务（friend/task.proto 依赖 user.proto，需先生成 user.proto）
protoc --proto_path=$PROJECT_DIR/example --cpp_out=$PROJECT_DIR/example $PROJECT_DIR/example/user.proto
protoc --proto_path=$PROJECT_DIR/example --cpp_out=$PROJECT_DIR/example $PROJECT_DIR/example/friend.proto
protoc --proto_path=$PROJECT_DIR/example --cpp_out=$PROJECT_DIR/example $PROJECT_DIR/example/task.proto

# ── 4. 编译项目 ──
echo ">>> Building project..."
rm -rf $PROJECT_DIR/build
mkdir -p $PROJECT_DIR/build
cd $PROJECT_DIR/build
cmake ..
make -j$(nproc)
cd $PROJECT_DIR

# ── 5. 拷贝头文件到 lib 目录 ──
echo ">>> Copying headers to lib..."
cp -r $PROJECT_DIR/src/include $PROJECT_DIR/lib

echo ""
echo "══════════════════════════════════════════════"
echo "  Build complete!"
echo "══════════════════════════════════════════════"
echo "  bin/provider   - RPC server (UserService + FriendService)"
echo "  bin/consumer   - RPC client (UserService::Login)"
echo "  lib/libmprpc.a - static library"
echo "══════════════════════════════════════════════"
echo ""
echo "Quick start:"
echo "  1. Start ZooKeeper:    sudo systemctl start zookeeper"
echo "  2. Start Redis:        redis-server &"
echo "  3. Start provider:     ./bin/provider -i test.conf"
echo "  4. Run consumer:       ./bin/consumer -i test.conf"
echo ""
