#include "base_test.hpp"

#include <unistd.h>
#include <sys/mman.h>

#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "../plugins/anti_caching/segment_manager/abstract_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/pmr_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/umap_segment_manager.hpp"
#include "../plugins/anti_caching/memory_resource/mmap_memory_resource.hpp"
#include "../plugins/anti_caching/segment_id.hpp"
#include "../plugins/anti_caching/segment_tools.hpp"
#include "gtest/gtest.h"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"

namespace opossum::anticaching {

class SegmentManagerTest : public BaseTest {
 public:
  SegmentManagerTest() {
  }

  ~SegmentManagerTest() {

  }

  inline static const std::string FILENAME = "umap_test_pool";
  inline static const size_t FILE_SIZE = 20ul * 1024 * 1024;
  // So segments will be around 1MB. +1 To make sure we are not a multiple of the page size (4096)
  static const size_t DEFAULT_ROW_COUNT = 256ul * 1024 + 1;

 protected:
  std::unique_ptr<AbstractSegmentManager> _create_segment_manager() {
    return std::make_unique<UmapSegmentManager>(FILENAME, FILE_SIZE);
//    _mmap = std::make_unique<MmapMemoryResource>(FILENAME, FILE_SIZE);
//    return std::make_unique<PmrSegmentManager>(*_mmap);
  }

  template<typename DataType>
  static bool equals(const BaseSegment& segment1, const BaseSegment& segment2) {
    if (segment1.size() != segment2.size()) {
      return false;
    }

    for (size_t i = 0ul, end = segment1.size(); i < end; ++i) {
      if (segment1[i] != segment2[i]) {
        return false;
      }
    }
    return true;
  }

  std::unique_ptr<MmapMemoryResource> _mmap;
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
  ASSERT_FALSE(stored_segment1);

  auto stored_segment2 = segment_manager->load(segment_id2);
  ASSERT_FALSE(stored_segment2);

  segment_manager->store(segment_id1, *SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT));
  ASSERT_TRUE(segment_manager->load(segment_id1));

  segment_manager->store(segment_id2, *SegmentTools::create_int_value_segment(DEFAULT_ROW_COUNT));
  ASSERT_TRUE(segment_manager->load(segment_id2));
}

TEST_F(SegmentManagerTest, BrutalSegmentSwapping) {
  auto segment_manager = _create_segment_manager();
  const auto& table = load_table("resources/test_data/tbl/int_equal_distribution.tbl", 3);
  // randomly select segment to select or evict
  const auto iterations = 1'000'000ul;
  const auto chunk_count = table->chunk_count();
  const auto column_count = table->column_count();
  auto sum = 0ul;
  auto segments_written_to_disk = 0;
  std::cout << "chunk_count: " << chunk_count << "\n";



//  for (auto c = 0u; c < chunk_count; ++c) {
//    auto chunk = table->get_chunk(ChunkID{c});
//    auto segment = chunk->get_segment(ColumnID{0});
//    const auto segment_id = SegmentID("test_table", ChunkID{c}, ColumnID{0}, "test_column");
//    auto copied_segment = segment_manager->store(segment_id, *segment);
//    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<int>>(copied_segment);
//    const auto iterable = create_iterable_from_segment<int>(*value_segment);
//    iterable.for_each([](const auto value) {std::cout << value.value() << "\n";});
//    std::cout.flush();
//  }
//  std::cout << "all done." << "\n";


  std::uniform_int_distribution<uint32_t> uniform_dist(0u, std::numeric_limits<uint32_t>::max());
  std::default_random_engine random_engine;

  for (auto i =0ul; i < iterations; ++i) {
//    if (i % 1 == 0) {
//      std::cout << "i=" << i << "\n";
//      std::cout.flush();
//    }

    if (i == 41) {
      ++sum;
    }
    const auto chunk_id = static_cast<ChunkID>(uniform_dist(random_engine) % chunk_count);
    const auto column_id = static_cast<ColumnID>(uniform_dist(random_engine) % column_count);

    const auto chunk = table->get_chunk(chunk_id);
    auto segment = chunk->get_segment(column_id);
    const auto segment_id = SegmentID("test_table", chunk_id, column_id, "test_column");

    std::shared_ptr<BaseSegment> new_segment{nullptr};

    if (uniform_dist(random_engine) & 1u) {
      // evict
      new_segment = segment_manager->load(segment_id);
      if (!new_segment) {
        new_segment = segment_manager->store(segment_id, *segment);
        std::cout << ++segments_written_to_disk << " segments written to disk.\n";
        std::cout.flush();
      }
    }
    else {
      // move to main memory
//      if (i == 42) {
//        const auto value_segment = std::dynamic_pointer_cast<ValueSegment<int>>(segment);
//        const auto iterable = create_iterable_from_segment<int>(*value_segment);
//        iterable.for_each([](const auto value) {std::cout << value.value() << "\n";});
//        std::cout.flush();
//      }
      new_segment = segment->copy_using_allocator({});
      segment_manager->remove(segment_id);
    }

    chunk->replace_segment(column_id, new_segment);
    segment = chunk->get_segment(column_id);

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<int>>(segment);
    const auto iterable = create_iterable_from_segment<int>(*value_segment);
    iterable.for_each([&sum](const auto value) {sum += value.value();});
  }

  std::cout << sum << "\n";

}

TEST_F(SegmentManagerTest, SwapStringDictionarySegment) {
  auto vs_str = std::make_shared<ValueSegment<pmr_string>>();
  const auto string_count = 65535;
  const auto max_string_length = 128;
  std::uniform_int_distribution<uint32_t> uniform_dist(0u, std::numeric_limits<uint32_t>::max());
  std::default_random_engine random_engine;

  for (auto i = 0u; i < string_count; ++i) {
    pmr_string str;
    for (auto c = 0u, end = uniform_dist(random_engine) % max_string_length; c < end; ++c) {
      str.append(1, static_cast<char>('a' + uniform_dist(random_engine) % 36));
    }
    vs_str->append(str);
  }

  auto segment =
    ChunkEncoder::encode_segment(vs_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned});

  auto segment_manager = std::make_unique<UmapSegmentManager>(FILENAME, FILE_SIZE);

  const char* m = "mathias\0";
  const char* a = "annika\0";
  const char* f = "florian\0";

  auto p1 = segment_manager->store((void*)m, 8);
  auto p2 = segment_manager->store((void*)a, 7);
  auto p3 = segment_manager->store((void*)f, 8);

  std::cout << (char*)p1 << (char*)p2 << (char*)p3;
  std::cout.flush();


  auto swapped_segment = segment_manager->store(SegmentID{"table_name", ChunkID{0}, ColumnID{0}, "column_name"}, *segment);
  auto swapped_segment2 = segment_manager->store(SegmentID{"table_name", ChunkID{1}, ColumnID{0}, "column_name"}, *segment);

  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment_manager->load(SegmentID{"table_name", ChunkID{0}, ColumnID{0}, "column_name"}));
  auto dict_segment2 = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment_manager->load(SegmentID{"table_name", ChunkID{1}, ColumnID{0}, "column_name"}));

//  EXPECT_TRUE(equals<pmr_string>(*dict_segment, *dict_segment2));


  // Test attribute_vector size
  EXPECT_EQ(segment->memory_usage(MemoryUsageCalculationMode::Full), swapped_segment->memory_usage(MemoryUsageCalculationMode::Full));
  EXPECT_EQ(segment->memory_usage(MemoryUsageCalculationMode::Full), swapped_segment2->memory_usage(MemoryUsageCalculationMode::Full));
  std::cout << "tests passed?" << "\n";
  std::cout.flush();
  std::cout.flush();
}


} // namespace opossum::anticaching
