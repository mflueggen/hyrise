#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

class KnapsackSolver {
 public:
  static std::vector<size_t>
  solve(const uint64_t memory_budget, const std::vector<float> values, const std::vector<uint64_t> costs);
};

}  // namespace opossum
