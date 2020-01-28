#include "persistent_memory_resource.hpp"

namespace opossum {

PersistentMemoryResource::PersistentMemoryResource(const std::string& pool_name, size_t pool_size)
  : pool_name{pool_name} {
  // TODO: Create or open
  _pool = pmem::obj::pool<struct root>::create(pool_name, "layout", PMEMOBJ_MIN_POOL * 4, S_IWRITE | S_IREAD);
  //    std::cout << pool_name << " created.\n";
}

void* PersistentMemoryResource::do_allocate(size_t bytes, size_t alignment) {
  void* ptr = nullptr;
//    std::cout << "allocating " << bytes << " bytes in pool " << pool_name << "\n";
//    pmem::obj::transaction::run(_pool, [&] {
//      auto persistent_ptr = pmem::obj::make_persistent<char[]>(bytes);
//      ptr = &persistent_ptr[0];
//      this->_pointers.insert({ptr, &persistent_ptr});
//    });
  return ptr;
}

void PersistentMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
//    std::cout << "deallocation of " << bytes << " requested for pool " << pool_name << "\n";
//    pmem::obj::transaction::run(_pool, [&] {
//      pmem::obj::persistent_ptr<char[]>* ptr = this->_pointers[p];
//      pmem::obj::delete_persistent<char[]>(*ptr, bytes);
//      this->_pointers.erase(ptr);
//    });
}

bool PersistentMemoryResource::do_is_equal(const std::pmr::memory_resource& __other) const noexcept {
  return true;
}

}  // namespace opossum
