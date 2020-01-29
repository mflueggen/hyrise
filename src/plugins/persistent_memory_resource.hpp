#pragma once

#include <unordered_map>

#include "boost/container/pmr/memory_resource.hpp"
#include "libpmemobj.h"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "types.hpp"

namespace opossum {

// not copyable, movable
class PersistentMemoryResource : private Noncopyable, public boost::container::pmr::memory_resource {
 public:
  PersistentMemoryResource(const std::string& name, size_t pool_size);

  const std::string name;
  const size_t pool_size;

 private:
  struct root {};
  pmem::obj::pool<struct root> _pool;
  std::unordered_map<void*, pmem::obj::persistent_ptr<char[]>*> _pointers;

  void* do_allocate(size_t bytes, size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;
};

}  // namespace opossum
