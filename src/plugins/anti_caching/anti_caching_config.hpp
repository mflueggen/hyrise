#pragma once

#include <limits>
#include <memory>
#include <string>

namespace opossum {

class AntiCachingConfig {
 public:
  AntiCachingConfig(uint64_t segment_eviction_interval_in_ms, uint64_t pool_size, uint64_t memory_budget,
                    const std::string& memory_resource_type);

  AntiCachingConfig(const std::string& filename);

  static AntiCachingConfig get_default_config();

  std::string to_string() const;

  static constexpr const char* PMEM_MEMORY_RESOURCE_TYPE = "pmem";
  static constexpr const char* MMAP_MEMORY_RESOURCE_TYPE = "mmap";

  uint64_t segment_eviction_interval_in_ms = 10'000ul;
  uint64_t pool_size = 4ul * 1024 * 1024;
  uint64_t memory_budget = std::numeric_limits<uint64_t>::max();
  std::string memory_resource_type = PMEM_MEMORY_RESOURCE_TYPE;

 private:
  AntiCachingConfig() = default;
};

} // namespace opossum
