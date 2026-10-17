#pragma once
#include <string>
#include <map>
#include <mutex>
#include <atomic>
#include <sstream>
#include <vector>
#include <cstdint>
#include <functional>
#include <thread>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>

namespace mprpc {

// 极简 Prometheus 指标导出器（无第三方依赖，裸 socket HTTP server）
// - Counter：只增计数
// - Gauge：瞬时值
// - Histogram：标准 Prometheus 直方图，预定义 le 累积桶，
//   输出 _bucket{le=...}/_sum/_count，可直接被 histogram_quantile() 消费
class MetricsExporter {
public:
    static MetricsExporter& GetInstance() {
        static MetricsExporter instance;
        return instance;
    }

    void Start(int port = 9090) {
        m_port = port;
        m_running = true;
        std::thread([this]() { this->ServerLoop(); }).detach();
    }

    void Stop() {
        m_running = false;
        if (m_server_fd >= 0) {
            close(m_server_fd);
            m_server_fd = -1;
        }
    }

    void IncrCounter(const std::string& name, const std::map<std::string, std::string>& labels = {}, double value = 1.0) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_counters[BuildKey(name, labels)] += value;
    }

    void SetGauge(const std::string& name, const std::map<std::string, std::string>& labels, double value) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_gauges[BuildKey(name, labels)] = value;
    }

    void ObserveHistogram(const std::string& name, const std::map<std::string, std::string>& labels, double value) {
        std::lock_guard<std::mutex> lock(m_mutex);
        Histogram& h = m_histograms[BuildKey(name, labels)];
        if (h.bucket_counts.empty()) h.bucket_counts.assign(kBucketCount, 0);
        h.count++;
        h.sum += value;
        for (size_t i = 0; i < kBucketCount; ++i) {
            if (value <= kBucketBounds[i]) {
                h.bucket_counts[i]++;
                break;
            }
        }
        if (value > kBucketBounds[kBucketCount - 1]) {
            h.above_last++;
        }
    }

    std::string RenderMetrics() {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::ostringstream oss;

        for (auto& [key, value] : m_counters) {
            oss << "# TYPE " << MetricName(key) << " counter\n";
            oss << key << " " << value << "\n";
        }

        for (auto& [key, value] : m_gauges) {
            oss << "# TYPE " << MetricName(key) << " gauge\n";
            oss << key << " " << value << "\n";
        }

        for (auto& [key, h] : m_histograms) {
            const std::string name = MetricName(key);
            const std::string labels = LabelSegment(key);
            oss << "# TYPE " << name << " histogram\n";
            uint64_t cumulative = 0;
            for (size_t i = 0; i < kBucketCount; ++i) {
                cumulative += h.bucket_counts[i];
                oss << name << "_bucket" << InsertLe(labels, FormatBound(kBucketBounds[i]))
                    << " " << cumulative << "\n";
            }
            cumulative += h.above_last;
            oss << name << "_bucket" << InsertLe(labels, "+Inf") << " " << cumulative << "\n";
            oss << name << "_sum" << labels << " " << h.sum << "\n";
            oss << name << "_count" << labels << " " << h.count << "\n";
        }

        return oss.str();
    }

private:
    MetricsExporter() : m_server_fd(-1), m_port(9090), m_running(false) {}

    // 直方图桶边界（秒），与 Prometheus client 默认值一致
    static constexpr double kBucketBounds[] = {
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
        0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0};
    static constexpr size_t kBucketCount = sizeof(kBucketBounds) / sizeof(kBucketBounds[0]);

    struct Histogram {
        // 每个桶存落入该桶（非累积）的计数，渲染时转累积
        std::vector<uint64_t> bucket_counts;
        uint64_t above_last = 0;  // 超出最大边界的样本数
        double sum = 0;
        uint64_t count = 0;
    };

    static std::string FormatBound(double b) {
        std::ostringstream oss;
        oss << b;
        return oss.str();
    }

    static std::string EscapeLabel(const std::string& v) {
        std::string out;
        out.reserve(v.size());
        for (char c : v) {
            if (c == '\\' || c == '"' || c == '\n') out += '\\';
            out += c;
        }
        return out;
    }

    std::string BuildKey(const std::string& name, const std::map<std::string, std::string>& labels) {
        std::ostringstream oss;
        oss << name << "{";
        bool first = true;
        for (auto& [k, v] : labels) {
            if (!first) oss << ",";
            oss << k << "=\"" << EscapeLabel(v) << "\"";
            first = false;
        }
        oss << "}";
        return oss.str();
    }

    // "name{k=\"v\"}" -> "name"
    static std::string MetricName(const std::string& key) {
        size_t pos = key.find('{');
        return pos == std::string::npos ? key : key.substr(0, pos);
    }

    // "name{k=\"v\"}" -> "{k=\"v\"}"（含花括号的标签段）
    static std::string LabelSegment(const std::string& key) {
        size_t pos = key.find('{');
        return pos == std::string::npos ? "{}" : key.substr(pos);
    }

    // 把 le 标签插到标签段最前：" {k=\"v\"}" -> "{le=\"0.1\",k=\"v\"}"
    static std::string InsertLe(const std::string& labelseg, const std::string& lv) {
        if (labelseg == "{}") return "{le=\"" + lv + "\"}";
        return "{le=\"" + lv + "\"," + labelseg.substr(1);
    }

    void ServerLoop() {
        m_server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (m_server_fd < 0) return;

        int opt = 1;
        setsockopt(m_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(m_port);

        if (bind(m_server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(m_server_fd);
            m_server_fd = -1;
            return;
        }

        listen(m_server_fd, 5);

        while (m_running) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(m_server_fd, (struct sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) continue;

            HandleClient(client_fd);
            close(client_fd);
        }
    }

    void HandleClient(int fd) {
        char buffer[4096] = {0};
        recv(fd, buffer, sizeof(buffer) - 1, 0);

        std::string metrics = RenderMetrics();

        std::string response = "HTTP/1.1 200 OK\r\n"
                              "Content-Type: text/plain; version=0.0.4\r\n"
                              "Content-Length: " + std::to_string(metrics.size()) + "\r\n"
                              "Connection: close\r\n"
                              "\r\n" + metrics;

        send(fd, response.c_str(), response.size(), 0);
    }

    int m_server_fd;
    int m_port;
    std::atomic<bool> m_running;

    mutable std::mutex m_mutex;
    std::map<std::string, double> m_counters;
    std::map<std::string, double> m_gauges;
    std::map<std::string, Histogram> m_histograms;
};

} // namespace mprpc
