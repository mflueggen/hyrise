#pragma once

#include <memory>

#include "boost/container/pmr/memory_resource.hpp"
#include "types.hpp"

namespace opossum::anticaching {

class MmapMemoryResource : private Noncopyable, public boost::container::pmr::memory_resource {
  friend class MmapMemoryResourceTest;
 public:
  MmapMemoryResource(const std::string& filename, size_t file_size);
  ~MmapMemoryResource() override;
  MmapMemoryResource(MmapMemoryResource&&) = delete; // WHY?
  MmapMemoryResource& operator=(MmapMemoryResource&&) = delete; // WHY?

  const std::string filename;
  const size_t file_size;

  size_t upper_file_pos = 0ul;

  char* mmap_pointer() const;
  int file_descriptor() const;

  void close_and_delete_file();

 private:
  char* _mmap_pointer;
  int _file_descriptor = -1;

  void _close_file();

  void* do_allocate(size_t bytes, size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;
};

}  // namespace opossum::anticaching
