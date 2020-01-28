#pragma once

#include <unordered_map>
#include <memory_resource>

#include "libpmemobj.h"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"

namespace opossum {

class PersistentMemoryResource : public std::pmr::memory_resource {
 public:
  PersistentMemoryResource(const std::string& pool_name, size_t pool_size);

  const std::string pool_name;
 private:
  struct root {};
  pmem::obj::pool<struct root> _pool;
  std::unordered_map<void*, pmem::obj::persistent_ptr<char[]>*> _pointers;

  void* do_allocate(size_t bytes, size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept override;

  bool do_is_equal(const memory_resource& __other) const noexcept override;
};

}  // namespace opossum
