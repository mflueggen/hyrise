#include "base_test.hpp"

#include <unistd.h>
#include <sys/mman.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../plugins/anti_caching/segment_manager/pmr_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/umap_segment_manager.hpp"
#include "../plugins/anti_caching/segment_id.hpp"
#include "../plugins/anti_caching/segment_tools.hpp"
#include "gtest/gtest.h"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
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
  // So segments will be around 1MB
  static const size_t DEFAULT_ROW_COUNT = 256ul * 1024;

 protected:
  std::unique_ptr<UmapSegmentManager> _create_segment_manager() {
    return std::make_unique<UmapSegmentManager>(FILENAME, FILE_SIZE);
  }

  template<typename DataType>
  static bool equals(const BaseSegment& segment1, const BaseSegment& segment2) {
    if (segment1.size() != segment2.size()) {
      return false;
    }

    for (size_t i = 0ul, end = segment1.size(); i < end; ++i) {
      if (segment1[i] != segment2[i]) return false;
    }
    return true;
  }

};

TEST_F(SegmentManagerTest, CreateAndDestroy) {
  auto segment_manager = _create_segment_manager();
}

TEST_F(SegmentManagerTest, Store1Segment) {
  auto segment_manager = _create_segment_manager();
  auto value_segment = SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT);

  auto segment_in_segment_manager = segment_manager->store(
    SegmentID("table1", ChunkID{0}, ColumnID{0}, "value_column1"), *value_segment);

  ASSERT_TRUE(equals<int32_t>(*value_segment, *segment_in_segment_manager));
  ++value_segment->values()[42];
  ASSERT_NE((*value_segment)[42], (*segment_in_segment_manager)[42]);
}

TEST_F(SegmentManagerTest, LoadAndStoreSegments) {
  const auto segment_count = 7u;
  auto segment_manager = _create_segment_manager();
  std::vector<std::shared_ptr<ValueSegment<int32_t>>> segments;
  std::vector<std::shared_ptr<BaseSegment>> segments_in_segment_manager;
  segments.reserve(segment_count);
  segments_in_segment_manager.reserve(segment_count);

  for (auto i = 0u; i < segment_count; ++i) {
    auto value_segment = SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT);
    segments.push_back(value_segment);
    segments_in_segment_manager.push_back(
      segment_manager->store(SegmentID("table1", ChunkID{i}, ColumnID{0}, "value_column" + std::to_string(i)),
                             *value_segment));
  }

  for (auto i = 0u; i < segment_count; ++i) {
    const auto expected_segment = segments[i];
    const auto stored_segment = segment_manager->load(
      SegmentID("table1", ChunkID{i}, ColumnID{0}, "value_column" + std::to_string(i)));
    ASSERT_TRUE(equals<int32_t>(*expected_segment, *stored_segment));
  }
}

TEST_F(SegmentManagerTest, Remove) {
  // Currently, remove just removes the segment from the active list and basically does nothing.
  auto segment_manager = _create_segment_manager();
  const auto segment_id = SegmentID{"table", ChunkID{0}, ColumnID{0}, "column"};
  ASSERT_FALSE(segment_manager->remove(segment_id));
  segment_manager->store(segment_id, *SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT));
  ASSERT_TRUE(segment_manager->remove(segment_id));
}

TEST_F(SegmentManagerTest, Load) {
  auto segment_manager = _create_segment_manager();
  const auto segment_id1 = SegmentID{"table", ChunkID{1}, ColumnID{0}, "column"};
  const auto segment_id2 = SegmentID{"table", ChunkID{2}, ColumnID{0}, "column"};

  auto stored_segment1 = segment_manager->load(segment_id1);
  ASSERT_EQ(*stored_segment1, nullptr);

  auto stored_segment2 = segment_manager->load(segment_id2);
  ASSERT_EQ(*stored_segment2, nullptr);

  segment_manager->store(segment_id1, *SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT));
  ASSERT_NE(*segment_manager->load(segment_id1), nullptr);

  segment_manager->store(segment_id2, *SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT));
  ASSERT_NE(*segment_manager->load(segment_id2), nullptr);
}


} // namespace opossum::anticaching
