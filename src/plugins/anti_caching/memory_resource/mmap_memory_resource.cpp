#include "mmap_memory_resource.hpp"

#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>

#include "../lib/utils/assert.hpp"


namespace opossum::anticaching {

MmapMemoryResource::MmapMemoryResource(const std::string& filename, size_t file_size)
  : filename{filename}, file_size{file_size} {
  // create a new file with file_size
  _file_descriptor = open(filename.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_SYNC | O_DSYNC, S_IRWXU);
  Assert(_file_descriptor >= 0, "Could not open/create file " + filename);
  std::filesystem::resize_file(filename, file_size);

  auto mmap_pointer = mmap(NULL, file_size, PROT_WRITE, MAP_SHARED, _file_descriptor, 0);
  Assert(_mmap_pointer != MAP_FAILED, "mmap failed.");
  _mmap_pointer = (char*)mmap_pointer;

  // After the mmap() call has returned, the file descriptor, fd, can be
  // closed immediately without invalidating the mapping.
  // The file is kept open so the umap memory resource can use it.
  // TODO: Is this really true?
}

MmapMemoryResource::~MmapMemoryResource() {
  _close_file();
}

char* MmapMemoryResource::mmap_pointer() const {
  return _mmap_pointer;
}

int MmapMemoryResource::file_descriptor() const {
  return _file_descriptor;
}

void MmapMemoryResource::_close_file() {
  if (_file_descriptor > -1) {
    Assert(!msync(_mmap_pointer, file_size, MS_SYNC), "msync failed with errno=" + std::to_string(errno));
    Assert(!munmap(_mmap_pointer, file_size), "munmap failed");
    close(_file_descriptor);
  }
  _file_descriptor = -1;
}

void MmapMemoryResource::close_and_delete_file() {
  if (_file_descriptor < 0) {
    return;
  }
  _close_file();
  Assert(std::filesystem::remove(filename), "Failed to remove " + filename);
}

void* MmapMemoryResource::do_allocate(size_t bytes, size_t alignment) {
  // round return_offset to possibly new alignment
  auto return_offset = upper_file_pos + (alignment - 1);
  return_offset -= return_offset % alignment;

  const auto new_upper_file_pos = return_offset + bytes;
  Assert(new_upper_file_pos < file_size, "Not enough free memory in mmap file.");

  upper_file_pos = new_upper_file_pos;
  return _mmap_pointer + return_offset;
}

void MmapMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
  std::cout << "MmapMemoryResource::do_deallocate was called but is not implemented\n";
}

bool MmapMemoryResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return (this == &other);
}

}  // namespace opossum::anticaching

