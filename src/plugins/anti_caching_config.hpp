#pragma once

#include <limits>
#include <memory>
#include <string>

class AntiCachingConfig {
 public:
  AntiCachingConfig(uint64_t segment_eviction_interval_in_ms, uint64_t pool_size, uint64_t memory_budget);

  static AntiCachingConfig get_default_config();

  std::string to_string() const;

  uint64_t segment_eviction_interval_in_ms = 10'000ul;
  uint64_t pool_size = 4ul * 1024 * 1024;
  uint64_t memory_budget = std::numeric_limits<uint64_t>::max();

 private:
  AntiCachingConfig() = default;
};


