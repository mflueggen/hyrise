#include "persistent_memory_manager.hpp"

namespace opossum {

PersistentMemoryManager& opossum::PersistentMemoryManager::get() {
    static PersistentMemoryManager persistent_memory_manager;
    return persistent_memory_manager;
}

size_t PersistentMemoryManager::create(size_t pool_size) {
    auto handle = _memory_resources.size();
    _memory_resources.emplace_back(
      std::make_shared<PersistentMemoryResource>("pool" + std::to_string(handle), pool_size));
    return handle;
}

std::shared_ptr<PersistentMemoryResource> PersistentMemoryManager::get(size_t handle) const {
    return _memory_resources[handle];
}

}  // namespace opossum
