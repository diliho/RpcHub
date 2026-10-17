#pragma once
#include "lockqueue.h"
#include <string>
#include <atomic>
#include <memory>
#include <cstdio>
#include <thread>

enum LogLevel
{
    INFO,
    WARN,
    ERROR,
};

class Logger
{
public:
    static Logger& GetInstance();
    void SetLogLevel(LogLevel level);
    void Log(LogLevel level, std::string msg);
    void Shutdown();

private:
    LockQueue<std::string> m_lockqueue;
    std::atomic<int> m_loglevel;
    std::atomic<bool> m_running;
    std::thread m_writer;

    Logger();
    ~Logger();
    Logger(const Logger&) = delete;
    Logger(Logger&&) = delete;
};

#define LOG_INFO(logmsgformat, ...) \
do \
{ \
    char buf[1024] = {0}; \
    snprintf(buf, sizeof(buf), logmsgformat, ##__VA_ARGS__); \
    Logger::GetInstance().Log(INFO, buf); \
} while(0);

#define LOG_WARN(logmsgformat, ...) \
do \
{ \
    char buf[1024] = {0}; \
    snprintf(buf, sizeof(buf), logmsgformat, ##__VA_ARGS__); \
    Logger::GetInstance().Log(WARN, buf); \
} while(0);

#define LOG_ERROR(logmsgformat, ...) \
do \
{ \
    char buf[1024] = {0}; \
    snprintf(buf, sizeof(buf), logmsgformat, ##__VA_ARGS__); \
    Logger::GetInstance().Log(ERROR, buf); \
} while(0);
