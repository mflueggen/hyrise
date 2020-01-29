#include "persistent_memory_manager.hpp"

namespace opossum {

PersistentMemoryManager& opossum::PersistentMemoryManager::get() {
    static PersistentMemoryManager persistent_memory_manager;
    return persistent_memory_manager;
}

size_t PersistentMemoryManager::create(size_t pool_size) {
    auto handle = _memory_resources.size();
    const auto pool_name = "pool" + std::to_string(handle);
    _memory_resources.emplace_back(std::make_unique<PersistentMemoryResource>(pool_name, pool_size));
    return handle;
}

PersistentMemoryResource& PersistentMemoryManager::get(size_t handle) const {
    return *_memory_resources[handle];
}

}  // namespace opossum
