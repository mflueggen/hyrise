#include "base_test.hpp"

#include <unistd.h>
#include <sys/mman.h>

#include <string>
#include <filesystem>

#include "gtest/gtest.h"
#include "../plugins/anti_caching/memory_resource/mmap_memory_resource.hpp"

namespace opossum::anticaching {

class MmapMemoryResourceTest : public BaseTest {
 public:
  MmapMemoryResourceTest() : _mmap_memory_resource{MmapMemoryResourceTest::FILENAME, FILE_SIZE} {
  }

  ~MmapMemoryResourceTest() {
    _mmap_memory_resource.close_and_delete_file();
  }

  inline static const std::string FILENAME = "mmap_test_pool";
  inline static const size_t FILE_SIZE = 10ul * 1024 * 1024;

 protected:
  MmapMemoryResource _mmap_memory_resource;

  void* _do_allocate(size_t bytes, size_t alignment) {
    return _mmap_memory_resource.do_allocate(bytes, alignment);
  }

};

TEST_F(MmapMemoryResourceTest, CloseAndDelete) {
  ASSERT_TRUE(std::filesystem::exists(FILENAME));
  ASSERT_TRUE(_mmap_memory_resource.file_descriptor() > -1);
  _mmap_memory_resource.close_and_delete_file();
  ASSERT_TRUE(_mmap_memory_resource.file_descriptor() < 0);
  ASSERT_FALSE(std::filesystem::exists(FILENAME));
}

TEST_F(MmapMemoryResourceTest, FileSize) {
  ASSERT_EQ(_mmap_memory_resource.file_size, MmapMemoryResourceTest::FILE_SIZE);
  ASSERT_EQ(std::filesystem::file_size(FILENAME), MmapMemoryResourceTest::FILE_SIZE);
}

TEST_F(MmapMemoryResourceTest, FileName) {
  ASSERT_EQ(_mmap_memory_resource.filename, MmapMemoryResourceTest::FILENAME);
}

TEST_F(MmapMemoryResourceTest, FileDescriptor) {
  ASSERT_GE(_mmap_memory_resource.file_descriptor(), 0);
  _mmap_memory_resource.close_and_delete_file();
  ASSERT_LT(_mmap_memory_resource.file_descriptor(), 0);
}

TEST_F(MmapMemoryResourceTest, DoAllocate) {
  const auto ALLOCATION_SIZE = 17ul;
  const auto ALIGNMENT = 13ul;

  ASSERT_EQ(_mmap_memory_resource.upper_file_pos, 0);
  const auto* mmap_pointer = _mmap_memory_resource.mmap_pointer();

  const auto* mmap_buffer1 = static_cast<char*>(_do_allocate(ALLOCATION_SIZE, ALIGNMENT));
  ASSERT_EQ(mmap_pointer, _mmap_memory_resource.mmap_pointer());
  ASSERT_EQ(_mmap_memory_resource.upper_file_pos, ALLOCATION_SIZE);

  const auto* mmap_buffer2 = static_cast<char*>(_do_allocate(ALLOCATION_SIZE, ALIGNMENT));
  ASSERT_EQ(mmap_pointer, _mmap_memory_resource.mmap_pointer());

  const auto pointer_difference = mmap_buffer2 - mmap_buffer1;
  ASSERT_EQ(pointer_difference % ALIGNMENT, 0);
  ASSERT_EQ(_mmap_memory_resource.upper_file_pos, 26 /* 17 rounded up to 26 */ + 17);
}

TEST_F(MmapMemoryResourceTest, PersistChanges) {
  const char* hyrise = "Hyrise";
  const auto ALLOCATION_SIZE = 17ul;
  const auto ALIGNMENT = 13ul;
  const auto OFFSET1 = 6ul;
  const auto BYTES_TO_READ = 6ul;
  const auto SIZE_OF_FILE_BUFFER = 512ul;
  const auto BLOCK_SIZE = 512ul;

  // required in case the file was opened using the O_DIRECT FLAG
  alignas(BLOCK_SIZE) char file_buffer[SIZE_OF_FILE_BUFFER];
  memset(file_buffer, 0, SIZE_OF_FILE_BUFFER);

  ASSERT_EQ(pread(_mmap_memory_resource.file_descriptor(), file_buffer, BLOCK_SIZE, 0), BLOCK_SIZE);

  for (auto i = 0ul; i < BYTES_TO_READ; ++i) {
    ASSERT_EQ(file_buffer[i + OFFSET1], 0);
  }

  auto* mmap_buffer = static_cast<char*>(_do_allocate(ALLOCATION_SIZE, ALIGNMENT));
  for (auto i = 0ul; i < BYTES_TO_READ; ++i) {
    mmap_buffer[i + OFFSET1] = hyrise[i];
  }

  // Make sure changes are persisted
  ASSERT_FALSE(msync(_mmap_memory_resource.mmap_pointer(), ALLOCATION_SIZE, MS_SYNC));
  ASSERT_EQ(pread(_mmap_memory_resource.file_descriptor(), file_buffer, BLOCK_SIZE, 0), BLOCK_SIZE);

  for (auto i = 0ul; i < BYTES_TO_READ; ++i) {
    ASSERT_EQ(file_buffer[i + OFFSET1], hyrise[i]);
  }
}

} // namespace opossum::anticaching