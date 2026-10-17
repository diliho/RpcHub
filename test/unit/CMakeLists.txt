# Unit tests for Mprpc-EE
include_directories(${PROJECT_SOURCE_DIR}/src/include)

# Test: Load Balancer
add_executable(test_load_balancer test_load_balancer.cc)
target_link_libraries(test_load_balancer pthread)

# Test: Memory Pool and Object Pool
add_executable(test_pool test_pool.cc)
target_link_libraries(test_pool pthread)

# Test: Lock-Free Queue
add_executable(test_lock_free_queue test_lock_free_queue.cc)
target_link_libraries(test_lock_free_queue pthread)

# Test: Circuit Breaker
add_executable(test_circuit_breaker test_circuit_breaker.cc)
target_link_libraries(test_circuit_breaker pthread)

# Test: Prometheus Metrics Exporter
add_executable(test_prometheus_exporter test_prometheus_exporter.cc)
target_link_libraries(test_prometheus_exporter pthread)
