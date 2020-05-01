#pragma once

#include <memory>

#include "boost/container/pmr/memory_resource.hpp"
#include "types.hpp"

namespace opossum::anticaching {

class LockableMemoryResource : private Noncopyable, public boost::container::pmr::memory_resource {
  friend class MmapMemoryResourceTest;
 public:
  LockableMemoryResource(size_t capacity);
  ~LockableMemoryResource() override;
  LockableMemoryResource(LockableMemoryResource&&) = delete; // WHY?
  LockableMemoryResource& operator=(LockableMemoryResource&&) = delete; // WHY?

  const size_t capacity;

  size_t size();

  void lock();
  void unlock();

  static const uint16_t PAGE_SIZE = 4096;

 private:
  size_t _size = 0;
  size_t _locked_size = 0;
  char* _memory_address = nullptr;

  void* do_allocate(size_t bytes, size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;
};

}  // namespace opossum::anticaching
