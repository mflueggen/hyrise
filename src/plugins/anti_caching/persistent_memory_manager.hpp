#pragma once

#include <memory>

#include "boost/container/pmr/memory_resource.hpp"
#include "types.hpp"

namespace opossum::anticaching {

class PersistentMemoryManager : private Noncopyable {
 public:
  static PersistentMemoryManager& get();

  size_t create_pmemobj(size_t pool_size);
  size_t create_mmap(size_t file_size);

  boost::container::pmr::memory_resource& get(size_t handle) const;

 private:
  std::vector<std::unique_ptr<boost::container::pmr::memory_resource>> _memory_resources;
};

}  // namespace opossum::anticaching
