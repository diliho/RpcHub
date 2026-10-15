#include "logger.h"
#include <ctime>
#include <iostream>
#include <thread>
#include <chrono>

Logger& Logger::GetInstance()
{
    static Logger logger;
    return logger;
}

Logger::Logger() : m_loglevel(INFO), m_running(true)
{
    std::thread writeLogTask([this]() {
        while (m_running)
        {
            std::string msg = m_lockqueue.Pop();
            if (msg.empty()) continue;

            time_t now = time(nullptr);
            tm* nowtm = localtime(&now);

            char file_name[128];
            snprintf(file_name, sizeof(file_name), "%d-%d-%d-log.txt",
                     nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday);

            FILE* pf = fopen(file_name, "a+");
            if (pf == nullptr)
            {
                std::cout << "logger file: " << file_name << " open error!" << std::endl;
                continue;
            }

            fputs(msg.c_str(), pf);
            fputs("\n", pf);
            fclose(pf);
        }
    });
    writeLogTask.detach();
}

void Logger::SetLogLevel(LogLevel level)
{
    m_loglevel.store(level);
}

void Logger::Log(LogLevel level, std::string msg)
{
    if (level < m_loglevel.load()) return;

    time_t now = time(nullptr);
    tm* nowtm = localtime(&now);

    const char* loglevel_str = "INFO";
    if (level == WARN) loglevel_str = "WARN";
    else if (level == ERROR) loglevel_str = "ERROR";

    char time_buf[128] = {0};
    snprintf(time_buf, sizeof(time_buf), "%d-%02d-%02d %02d:%02d:%02d [%s] ",
             nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday,
             nowtm->tm_hour, nowtm->tm_min, nowtm->tm_sec, loglevel_str);
    msg.insert(0, time_buf);

    m_lockqueue.Push(msg);
}

void Logger::Shutdown()
{
    m_running = false;
    m_lockqueue.Shutdown();  // 唤醒阻塞在 Pop() 上的后台线程，令其排空队列后退出
}
