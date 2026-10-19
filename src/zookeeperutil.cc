#include "zookeeperutil.h"
#include "mprpcapplication.h"
#include <iostream>
#include "logger.h"
#include "rpcerror.h"

void global_watcher(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)
{
    if (type != ZOO_SESSION_EVENT) return;
    ZKClient* client = static_cast<ZKClient*>(watcherCtx);
    if (!client) return;

    if (state == ZOO_CONNECTED_STATE)
    {
        LOG_INFO("ZK connected successfully");
        sem_post(client->getConnectSem());
    }
    else if (state == ZOO_CONNECTING_STATE)
    {
        LOG_INFO("ZK connecting...");
    }
    else if (state == ZOO_EXPIRED_SESSION_STATE)
    {
        LOG_ERROR("ZK session expired, reinitializing...");
        // doReinitialize may block, offload to a separate thread
        // to avoid deadlocking the ZK event thread
        std::thread t([client]() { client->doReinitialize(); });
        t.detach();
    }
    else if (state == ZOO_AUTH_FAILED_STATE)
    {
        LOG_ERROR("ZK auth failed");
    }
}

ZKClient::ZKClient() : m_zhandle(nullptr), m_connected(false)
{
    sem_init(&m_sem, 0, 0);
}

ZKClient::~ZKClient()
{
    if (m_zhandle != nullptr)
    {
        zookeeper_close(m_zhandle);
    }
    sem_destroy(&m_sem);
}

bool ZKClient::Start()
{
    std::lock_guard<std::mutex> lock(m_zk_mtx);
    if (m_zhandle != nullptr)
    {
        // Already started (shared ZKClient case)
        return m_connected;
    }

    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    std::string conststr = host + ":" + port;

    if (host.empty() || port.empty())
    {
        LOG_ERROR("ZK config not found (zookeeperip/zookeeperport)");
        return false;
    }

    m_zhandle = zookeeper_init(conststr.c_str(), global_watcher, 3000, nullptr, nullptr, 0);
    if (nullptr == m_zhandle)
    {
        LOG_ERROR("zookeeper_init error");
        return false;
    }

    zoo_set_context(m_zhandle, this);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 5;
    int ret = sem_timedwait(&m_sem, &ts);
    if (ret == -1)
    {
        LOG_ERROR("ZK connect timeout (5s)");
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
        return false;
    }

    m_connected = true;
    LOG_INFO("zookeeper_init success");
    return true;
}

void ZKClient::doReinitialize()
{
    {
        std::lock_guard<std::mutex> lock(m_zk_mtx);
        if (m_zhandle)
        {
            zookeeper_close(m_zhandle);
            m_zhandle = nullptr;
        }
        m_connected = false;
        sem_destroy(&m_sem);
        sem_init(&m_sem, 0, 0);
    }

    LOG_INFO("ZK reconnecting...");
    if (Start())
    {
        LOG_INFO("ZK reconnected");
        std::vector<ReconnectCallback> cbs;
        {
            std::lock_guard<std::mutex> lock(m_zk_mtx);
            cbs = m_reconnect_cbs;
        }
        for (auto& cb : cbs)
        {
            if (cb) cb();
        }
    }
    else
    {
        LOG_ERROR("ZK reconnection failed, will retry later");
    }
}

void ZKClient::setOnReconnectCallback(ReconnectCallback cb)
{
    std::lock_guard<std::mutex> lock(m_zk_mtx);
    m_reconnect_cbs.clear();
    m_reconnect_cbs.push_back(std::move(cb));
}

void ZKClient::addReconnectCallback(ReconnectCallback cb)
{
    std::lock_guard<std::mutex> lock(m_zk_mtx);
    m_reconnect_cbs.push_back(std::move(cb));
}

bool ZKClient::isConnected()
{
    std::lock_guard<std::mutex> lock(m_zk_mtx);
    return m_connected;
}

void ZKClient::CreateParentNodes(const char* path)
{
    if (!m_zhandle || !m_connected) return;
    std::string parent_path(path);
    size_t last_slash = parent_path.find_last_of('/');
    if (last_slash == 0) return;

    parent_path = parent_path.substr(0, last_slash);
    int flag = zoo_exists(m_zhandle, parent_path.c_str(), 0, nullptr);
    if (flag == ZNONODE)
    {
        CreateParentNodes(parent_path.c_str());
        flag = zoo_create(m_zhandle, parent_path.c_str(), nullptr, 0, &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
        if (flag == ZOK)
        {
            LOG_INFO("SUCCESS: Create parent node: %s", parent_path.c_str());
        }
        else
        {
            LOG_ERROR("Create parent node failed: %s, error code: %d", parent_path.c_str(), flag);
        }
    }
}

void ZKClient::Create(const char* path, const char* data, int datalen, int state,
                      char* result_path, int result_path_len)
{
    if (!m_zhandle || !m_connected)
    {
        LOG_ERROR("ZK not connected, cannot create node: %s", path);
        return;
    }

    CreateParentNodes(path);

    char path_buffer[256];
    int bufferlen = sizeof(path_buffer);
    char* out_buf = result_path ? result_path : path_buffer;
    int out_len = result_path && result_path_len > 0 ? result_path_len : bufferlen;

    int flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (ZNONODE == flag)
    {
        flag = zoo_create(m_zhandle, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE,
                          state, out_buf, out_len);
        if (flag == ZOK)
        {
            LOG_INFO("SUCCESS: Create znode: %s data: %s", path, data ? data : "null");
        }
        else
        {
            LOG_ERROR("Create znode failed: %s, error code: %d", path, flag);
        }
    }
    else
    {
        // Node already exists — for sequential nodes, this shouldn't happen,
        // but handle gracefully. For non-sequential, update data.
        if (state & ZOO_SEQUENCE)
        {
            // Sequential node path doesn't exist yet, force create
            flag = zoo_create(m_zhandle, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE,
                              state, out_buf, out_len);
            if (flag == ZOK)
            {
                LOG_INFO("SUCCESS: Create sequential znode: %s data: %s", path, data ? data : "null");
            }
            else
            {
                LOG_ERROR("Create sequential znode failed: %s, error code: %d", path, flag);
            }
        }
        else
        {
            flag = zoo_set(m_zhandle, path, data, datalen, -1);
            if (flag == ZOK)
            {
                LOG_INFO("Update znode data: %s", path);
            }
            else
            {
                LOG_ERROR("Update znode data failed: %s, error code: %d", path, flag);
            }
        }
    }
}

std::string ZKClient::GetData(const char* path)
{
    if (m_zhandle == nullptr)
    {
        LOG_ERROR("Zookeeper handle is null, cannot get data for path: %s", path);
        return "";
    }

    char buffer[64];
    int bufferlen = sizeof(buffer);
    int flag = zoo_get(m_zhandle, path, 0, buffer, &bufferlen, nullptr);

    if (flag != ZOK)
    {
        LOG_ERROR("Get znode failed, path: %s error code: %d", path, flag);
        return "";
    }
    else
    {
        LOG_INFO("SUCCESS: Get znode data, path: %s", path);
        return buffer;
    }
}

std::vector<std::string> ZKClient::GetChildren(const char* path)
{
    std::vector<std::string> children;
    if (m_zhandle == nullptr)
    {
        LOG_ERROR("Zookeeper handle is null, cannot get children for path: %s", path);
        return children;
    }

    struct String_vector str_vec;
    int flag = zoo_get_children(m_zhandle, path, 0, &str_vec);
    if (flag == ZOK)
    {
        for (int i = 0; i < str_vec.count; ++i)
        {
            children.push_back(str_vec.data[i]);
        }
        deallocate_String_vector(&str_vec);
        LOG_INFO("SUCCESS: Get children of %s, count=%d", path, (int)children.size());
    }
    else
    {
        LOG_ERROR("Get children failed, path: %s error code: %d", path, flag);
    }
    return children;
}

bool ZKClient::WGet(const char* path, std::string& data, watcher_fn fn, void* ctx)
{
    if (!m_zhandle || !m_connected) return false;

    // 动态 buffer： GetData 的定长 64B 不够通用
    static const int INIT_LEN = 256;
    std::vector<char> buffer(INIT_LEN);
    int bufferlen = INIT_LEN;
    int flag = zoo_wget(m_zhandle, path, fn, ctx, buffer.data(), &bufferlen, nullptr);

    if (flag == ZOK)
    {
        data.assign(buffer.data(), bufferlen > 0 ? bufferlen : 0);
        return true;
    }
    if (flag == ZNONODE) return false;

    // buffer 太小则按实际长度重试一次（不带 watch，避免语义混乱）
    if (flag == ZMARSHALLINGERROR || bufferlen > INIT_LEN)
    {
        std::vector<char> big(bufferlen > 0 ? bufferlen : INIT_LEN * 4);
        int biglen = big.size();
        if (zoo_get(m_zhandle, path, 0, big.data(), &biglen, nullptr) == ZOK)
        {
            data.assign(big.data(), biglen > 0 ? biglen : 0);
            return true;
        }
    }
    LOG_ERROR("ZK WGet failed, path: %s error: %d", path, flag);
    return false;
}

bool ZKClient::WGetChildren(const char* path, std::vector<std::string>& children,
                            watcher_fn fn, void* ctx)
{
    if (!m_zhandle || !m_connected) return false;

    struct String_vector str_vec;
    int flag = zoo_wget_children(m_zhandle, path, fn, ctx, &str_vec);
    if (flag != ZOK)
    {
        if (flag != ZNONODE)
            LOG_ERROR("ZK WGetChildren failed, path: %s error: %d", path, flag);
        return false;
    }
    children.clear();
    for (int i = 0; i < str_vec.count; ++i)
        children.push_back(str_vec.data[i]);
    deallocate_String_vector(&str_vec);
    return true;
}

bool ZKClient::Delete(const char* path)
{
    if (!m_zhandle || !m_connected) return false;
    int flag = zoo_delete(m_zhandle, path, -1);
    if (flag != ZOK && flag != ZNONODE)
    {
        LOG_ERROR("ZK delete failed, path: %s error: %d", path, flag);
        return false;
    }
    return true;
}
