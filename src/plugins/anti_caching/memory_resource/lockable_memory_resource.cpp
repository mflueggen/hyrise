#include "lockable_memory_resource.hpp"

#include <cstdlib>
#include <cstring>
#include <sys/mman.h>

#include "../lib/utils/assert.hpp"


namespace opossum::anticaching {

LockableMemoryResource::LockableMemoryResource(size_t capacity)
  : capacity{capacity} {
  Assert(capacity % PAGE_SIZE == 0, "capacity must be divisible by page size.");
  _memory_address = (char*)std::malloc(capacity);
  if (!_memory_address) {
    Fail("malloc failed with: " + std::strerror(errno));
  }
}

LockableMemoryResource::~LockableMemoryResource() {
  unlock();
//  std::free(_memory_address);
}

size_t LockableMemoryResource::size() {
  return _size;
}

void LockableMemoryResource::lock() {
  if (mlock(_memory_address, _size)) {
    Fail("mlock failed with: " + std::strerror(errno));
  }
}

void LockableMemoryResource::unlock() {
  if (munlock(_memory_address, _size)) {
    Fail("munlock failed with: " + std::strerror(errno));
  }
}

void* LockableMemoryResource::do_allocate(size_t bytes, size_t alignment) {
  // round current size up to alignment
  _size += alignment - 1;
  _size -= _size % alignment;

  const auto return_address = _memory_address + _size;

  _size += bytes;
  Assert(_size < capacity, "Not enough free memory in LockableMemoryResource.");

  return return_address;
}

void LockableMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
//  std::cout << "MmapMemoryResource::do_deallocate was called but is not implemented\n";
}

bool LockableMemoryResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return (this == &other);
}

}  // namespace opossum::anticaching

