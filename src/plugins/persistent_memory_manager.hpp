#pragma once

#include <memory>

#include "boost/container/pmr/memory_resource.hpp"
#include "types.hpp"

namespace opossum {

class PersistentMemoryManager : private Noncopyable {
 public:
  static PersistentMemoryManager& get();

  size_t create(size_t pool_size);

  boost::container::pmr::memory_resource& get(size_t handle) const;

 private:
  std::vector<std::unique_ptr<boost::container::pmr::memory_resource>> _memory_resources;
};

}  // namespace opossum
