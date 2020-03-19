#include "base_test.hpp"

#include <unistd.h>
#include <sys/mman.h>

#include <memory>
#include <string>
#include <filesystem>

#include "../plugins/anti_caching/segment_manager/pmr_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/umap_segment_manager.hpp"
#include "../plugins/anti_caching/segment_id.hpp"
#include "gtest/gtest.h"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum::anticaching {

class SegmentManagerTest : public BaseTest {
 public:
  SegmentManagerTest() {
  }

  ~SegmentManagerTest() {

  }

  inline static const std::string FILENAME = "umap_test_pool";
  inline static const size_t FILE_SIZE = 10ul * 1024 * 1024;

 protected:
  std::unique_ptr<UmapSegmentManager> _create_segment_manager() {
    return std::make_unique<UmapSegmentManager>(FILENAME, FILE_SIZE);
  }

};

TEST_F(SegmentManagerTest, CreateAndDestroy) {
  auto segment_manager = _create_segment_manager();
}

TEST_F(SegmentManagerTest, Store) {
  auto segment_manager = _create_segment_manager();
  auto value_segment = ValueSegment<int>();
  for (auto i = 0; i < 32 * 1024; ++i) {
    value_segment.append(i);
  }

  auto segment_in_segment_manager = segment_manager->store(
    SegmentID("table1", ChunkID{0}, ColumnID{0}, "value_column1"), value_segment);

  for (auto i = 0; i < 32 * 1024; ++i) {
    ASSERT_EQ(value_segment[i], (*segment_in_segment_manager)[i]);
  }
}

TEST_F(SegmentManagerTest, Remove) {
}

TEST_F(SegmentManagerTest, Load) {
}



} // namespace opossum::anticaching
