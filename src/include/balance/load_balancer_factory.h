#pragma once
#include "load_balancer.h"
#include "round_robin.h"
#include "weighted_round_robin.h"
#include "consistent_hash.h"
#include "least_connections.h"
#include <memory>
#include <string>

namespace mprpc
{

    class LoadBalancerFactory
    {
    public:
        static std::unique_ptr<LoadBalancer> Create(const std::string &algorithm)
        {
            if (algorithm == "weighted_round_robin")
            {
                return std::make_unique<WeightedRoundRobinBalancer>();
            }
            else if (algorithm == "consistent_hash")
            {
                return std::make_unique<ConsistentHashBalancer>();
            }
            else if (algorithm == "least_connections")
            {
                return std::make_unique<LeastConnectionsBalancer>();
            }
            else
            {
                return std::make_unique<RoundRobinBalancer>();
            }
        }
    };

} // namespace mprpc
