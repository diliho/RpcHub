#include <iostream>
#include <cassert>
#include <string>
#include "prometheus/metrics_exporter.h"

using mprpc::MetricsExporter;

void TestCounter() {
    std::cout << "Testing MetricsExporter Counter..." << std::endl;

    std::map<std::string, std::string> labels = {{"service", "User"}, {"method", "Login"}};
    MetricsExporter::GetInstance().IncrCounter("mprpc_test_calls_total", labels);
    MetricsExporter::GetInstance().IncrCounter("mprpc_test_calls_total", labels);

    std::string rendered = MetricsExporter::GetInstance().RenderMetrics();
    assert(rendered.find("mprpc_test_calls_total{method=\"Login\",service=\"User\"} 2") != std::string::npos);
    assert(rendered.find("# TYPE mprpc_test_calls_total counter") != std::string::npos);

    std::cout << "MetricsExporter Counter: PASSED" << std::endl;
}

void TestGauge() {
    std::cout << "Testing MetricsExporter Gauge..." << std::endl;

    std::map<std::string, std::string> labels = {{"service", "User"}};
    MetricsExporter::GetInstance().SetGauge("mprpc_test_queue_size", labels, 42.5);

    std::string rendered = MetricsExporter::GetInstance().RenderMetrics();
    assert(rendered.find("mprpc_test_queue_size{service=\"User\"} 42.5") != std::string::npos);

    std::cout << "MetricsExporter Gauge: PASSED" << std::endl;
}

void TestHistogram() {
    std::cout << "Testing MetricsExporter Histogram..." << std::endl;

    std::map<std::string, std::string> labels = {{"service", "User"}, {"method", "Login"}};
    // 观测 4 个值：0.003(<=0.005), 0.02(<=0.025), 0.4(<=0.5), 2.0(<=2.5)
    MetricsExporter::GetInstance().ObserveHistogram("mprpc_test_duration_seconds", labels, 0.003);
    MetricsExporter::GetInstance().ObserveHistogram("mprpc_test_duration_seconds", labels, 0.02);
    MetricsExporter::GetInstance().ObserveHistogram("mprpc_test_duration_seconds", labels, 0.4);
    MetricsExporter::GetInstance().ObserveHistogram("mprpc_test_duration_seconds", labels, 2.0);

    std::string rendered = MetricsExporter::GetInstance().RenderMetrics();

    // 累积桶：le=0.005 -> 1, le=0.025 -> 2, le=0.5 -> 3, le=2.5 -> 4, +Inf -> 4
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"0.005\",method=\"Login\",service=\"User\"} 1") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"0.025\",method=\"Login\",service=\"User\"} 2") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"0.5\",method=\"Login\",service=\"User\"} 3") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"2.5\",method=\"Login\",service=\"User\"} 4") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"+Inf\",method=\"Login\",service=\"User\"} 4") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_bucket{le=\"0.01\",method=\"Login\",service=\"User\"} 1") != std::string::npos);

    // _sum / _count
    assert(rendered.find("mprpc_test_duration_seconds_sum{method=\"Login\",service=\"User\"} 2.423") != std::string::npos);
    assert(rendered.find("mprpc_test_duration_seconds_count{method=\"Login\",service=\"User\"} 4") != std::string::npos);
    assert(rendered.find("# TYPE mprpc_test_duration_seconds histogram") != std::string::npos);

    std::cout << "MetricsExporter Histogram: PASSED" << std::endl;
}

void TestLabelEscape() {
    std::cout << "Testing MetricsExporter Label Escape..." << std::endl;

    std::map<std::string, std::string> labels = {{"method", "Do\"Stuff"}};
    MetricsExporter::GetInstance().IncrCounter("mprpc_test_escape_total", labels);

    std::string rendered = MetricsExporter::GetInstance().RenderMetrics();
    assert(rendered.find("method=\"Do\\\"Stuff\"") != std::string::npos);

    std::cout << "MetricsExporter Label Escape: PASSED" << std::endl;
}

int main() {
    TestCounter();
    TestGauge();
    TestHistogram();
    TestLabelEscape();

    std::cout << "\nAll metrics exporter tests PASSED!" << std::endl;
    return 0;
}
