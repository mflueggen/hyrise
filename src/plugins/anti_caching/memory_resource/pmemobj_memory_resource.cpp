#include "pmemobj_memory_resource.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

namespace opossum::anticaching {

PmemObjMemoryResource::PmemObjMemoryResource(const std::string& name, size_t pool_size)
  : name{name}, pool_size(pool_size)
{
  // if pool exists, we delete it.
  if (std::filesystem::exists(name)) std::filesystem::remove(name);
  _pool = pmem::obj::pool<root>::create(name, "layout", pool_size, S_IWRITE | S_IREAD);
}

void* PmemObjMemoryResource::do_allocate(size_t bytes, size_t alignment) {
  void* ptr = nullptr;
    std::cout << "allocating " << bytes << " bytes in pool " << 0 << "\n";
    pmem::obj::transaction::run(_pool, [&] {
      auto persistent_ptr = pmem::obj::make_persistent<char[]>(bytes);
      ptr = &persistent_ptr[0];
      this->_pointers.insert({ptr, &persistent_ptr});
    });
  return ptr;
}

void PmemObjMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
    std::cout << "deallocation of " << bytes << " requested for pool " << 0 << "\n";
    pmem::obj::transaction::run(_pool, [&] {
      pmem::obj::persistent_ptr<char[]>* ptr = this->_pointers[p];
      pmem::obj::delete_persistent<char[]>(*ptr, bytes);
      this->_pointers.erase(ptr);
    });
}

bool PmemObjMemoryResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return (this == &other);
}

}  // namespace opossum::anticaching
