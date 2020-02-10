#include "persistent_memory_manager.hpp"

#include "boost/container/pmr/monotonic_buffer_resource.hpp"
#include "mmap_memory_resource.hpp"
#include "pmemobj_memory_resource.hpp"


namespace opossum {

PersistentMemoryManager& opossum::PersistentMemoryManager::get() {
  static PersistentMemoryManager persistent_memory_manager;
  return persistent_memory_manager;
}

size_t PersistentMemoryManager::create_pmemobj(size_t pool_size) {
  auto handle = _memory_resources.size();
  const auto pool_name = "pmem_pool" + std::to_string(handle);

  auto upstream = std::make_unique<PmemObjMemoryResource>(pool_name, pool_size);
  auto monotonic_memory_resource = std::make_unique<boost::container::pmr::monotonic_buffer_resource>(&(*upstream));

  _memory_resources.emplace_back(std::move(monotonic_memory_resource));
  _memory_resources.emplace_back(std::move(upstream));

  return handle;
}

size_t PersistentMemoryManager::create_mmap(size_t file_size) {
  auto handle = _memory_resources.size();
  const auto filename = "mmap_pool" + std::to_string(handle);

  auto upstream = std::make_unique<MmapMemoryResource>(filename, file_size);
  auto monotonic_memory_resource = std::make_unique<boost::container::pmr::monotonic_buffer_resource>(&(*upstream));

  _memory_resources.emplace_back(std::move(monotonic_memory_resource));
  _memory_resources.emplace_back(std::move(upstream));

  return handle;
}

boost::container::pmr::memory_resource& PersistentMemoryManager::get(size_t handle) const {
  return *_memory_resources[handle];
}

}  // namespace opossum
