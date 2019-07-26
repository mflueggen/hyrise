#pragma once

#include <vector>

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

class AbstractJittable;

void place_and_create_jit_reader_wrappers(std::vector<std::shared_ptr<AbstractJittable>>& jit_operators);

void connect_jit_operators(std::vector<std::shared_ptr<AbstractJittable>>& jit_operators);

template <typename T>
std::string type_to_string() {
  if constexpr (std::is_same_v<T, bool>) {
    return "Bool";
  } else if constexpr (std::is_same_v<T, ValueID>) {
    return "ValueID";
  }
  return data_type_to_string.left.at(data_type_from_type<T>());
}

}  // namespace opossum
