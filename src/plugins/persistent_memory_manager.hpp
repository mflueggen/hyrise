#pragma once

#include <memory>

#include "persistent_memory_resource.hpp"
#include "types.hpp"

namespace opossum {

class PersistentMemoryManager : private Noncopyable {
 public:
  static PersistentMemoryManager& get();

  size_t create(size_t pool_size);

  std::shared_ptr<PersistentMemoryResource> get(size_t handle) const;

 private:
  std::vector <std::shared_ptr<PersistentMemoryResource>> _memory_resources;
};

}  // namespace opossum
