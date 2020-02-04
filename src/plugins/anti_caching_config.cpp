#include "anti_caching_config.hpp"

#include "boost/format.hpp"

AntiCachingConfig::AntiCachingConfig(uint64_t segment_eviction_interval_in_ms, uint64_t pool_size,
                                     uint64_t memory_budget)
  : segment_eviction_interval_in_ms{segment_eviction_interval_in_ms}, pool_size{pool_size},
    memory_budget{memory_budget} {}

AntiCachingConfig AntiCachingConfig::get_default_config() {
  return AntiCachingConfig();
}

std::string AntiCachingConfig::to_string() const {
  return (boost::format("segment_eviction_interval_in_ms: %d, pool_size: %d, memory_budget: %d") %
          segment_eviction_interval_in_ms % pool_size % memory_budget).str();
}