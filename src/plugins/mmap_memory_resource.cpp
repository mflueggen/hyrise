#include "mmap_memory_resource.hpp"

#include <cstdio>
#include <filesystem>
#include <sys/mman.h>

#include "../lib/utils/assert.hpp"


namespace opossum {

MmapMemoryResource::MmapMemoryResource(const std::string& name, size_t file_size)
  : name{name}, file_size{file_size}, _upper_file_pos{0} {
  // create a new file with file_size
  auto file = std::fopen(name.c_str(), "w+");
  // auto file = open64(name, O_RDWR | O_TRUNC, O_DIRECT | O_DSYNC);
  Assert(file, "Could not open/create file " + name);
  std::filesystem::resize_file(name, file_size);

  auto mmap_pointer = mmap64(NULL, file_size, PROT_WRITE, MAP_SHARED, file->_fileno, 0);
  Assert(_mmap_pointer != MAP_FAILED, "mmap failed.");
  _mmap_pointer = (char*)mmap_pointer;

  //  After the mmap() call has returned, the file descriptor, fd, can be
  //  closed immediately without invalidating the mapping.
  std::fclose(file);
}

void* MmapMemoryResource::do_allocate(size_t bytes, size_t alignment) {
  // round _upper_file_pos to possibly new alignment
  const auto return_offset = _upper_file_pos + _upper_file_pos % alignment;
  const auto new_upper_file_pos = return_offset + bytes;
  Assert(new_upper_file_pos < file_size, "Not enough free memory in mmap file.");

  _upper_file_pos = new_upper_file_pos;
  return _mmap_pointer + return_offset;
}

void MmapMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
  std::cout << "MmapMemoryResource::do_deallocate was called but is not implemented\n";
}

bool MmapMemoryResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return (this == &other);
}

}  // namespace opossum

