#pragma once

#include <memory>

#include "boost/container/pmr/memory_resource.hpp"
#include "types.hpp"

namespace opossum {

class MmapMemoryResource : private Noncopyable, public boost::container::pmr::memory_resource {
 public:
  MmapMemoryResource(const std::string& filename, size_t file_size);

  const std::string filename;
  const size_t file_size;

 private:
  char* _mmap_pointer;
  size_t _upper_file_pos;

  void* do_allocate(size_t bytes, size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;
};

}  // namespace opossum
