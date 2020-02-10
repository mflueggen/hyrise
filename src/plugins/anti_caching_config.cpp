#include "anti_caching_config.hpp"

#include <filesystem>
#include <fstream>

#include "../../third_party/nlohmann_json/single_include/nlohmann/json.hpp"
#include "../lib/utils/assert.hpp"
#include "boost/format.hpp"

AntiCachingConfig::AntiCachingConfig(uint64_t segment_eviction_interval_in_ms, uint64_t pool_size,
                                     uint64_t memory_budget, const std::string& memory_resource_type)
  : segment_eviction_interval_in_ms{segment_eviction_interval_in_ms}, pool_size{pool_size},
    memory_budget{memory_budget}, memory_resource_type{memory_resource_type} {}

AntiCachingConfig::AntiCachingConfig(const std::string& filename) {
  Assert(std::filesystem::exists(filename), "Cannot load AntiCachingConfig from " + filename +
                                            ". File does not exist.");
  std::ifstream config_file(filename);
  nlohmann::json json_config;
  config_file >> json_config;
  memory_budget = json_config.value("memory_budget", memory_budget);
  pool_size = json_config.value("pool_size", pool_size);
  segment_eviction_interval_in_ms = json_config.value("segment_eviction_interval_in_ms",
    segment_eviction_interval_in_ms);
  memory_resource_type = json_config.value("memory_resource_type", memory_resource_type);
}

AntiCachingConfig AntiCachingConfig::get_default_config() {
  return AntiCachingConfig();
}

std::string AntiCachingConfig::to_string() const {
  return (boost::format("segment_eviction_interval_in_ms: %d, pool_size: %d, memory_budget: %d, "
                        "memory_resource_type: %s") %
          segment_eviction_interval_in_ms % pool_size % memory_budget % memory_resource_type).str();
}