#pragma once
#include <semaphore.h>
#include <zookeeper/zookeeper.h>
#include <string>
#include <functional>
#include <vector>
#include <mutex>

class ZKClient
{
public:
    ZKClient();
    ~ZKClient();
    bool Start();
    void Create(const char* path, const char* data, int datalen, int state=0,
                char* result_path=nullptr, int result_path_len=0);
    std::string GetData(const char* path);
    std::vector<std::string> GetChildren(const char* path);

    // 带 watch 的读取：节点/子节点变化时触发 fn（在 ZK 事件线程回调，
    // 回调内只做轻量通知，勿阻塞）。watch 一次性，触发后需重新注册。
    bool WGet(const char* path, std::string& data, watcher_fn fn, void* ctx);
    bool WGetChildren(const char* path, std::vector<std::string>& children,
                      watcher_fn fn, void* ctx);
    bool Delete(const char* path);

    zhandle_t* getHandle() { return m_zhandle; }
    bool isConnected();
    sem_t* getConnectSem() { return &m_sem; }

    using ReconnectCallback = std::function<void()>;
    void setOnReconnectCallback(ReconnectCallback cb);
    void addReconnectCallback(ReconnectCallback cb);
    void doReinitialize();

private:
    zhandle_t* m_zhandle;
    sem_t m_sem;
    bool m_connected;
    std::vector<ReconnectCallback> m_reconnect_cbs;
    std::mutex m_zk_mtx;

    void CreateParentNodes(const char* path);
};
