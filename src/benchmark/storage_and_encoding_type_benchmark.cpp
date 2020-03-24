#include <filesystem>
#include <memory>
#include <string>

#include <boost/container/pmr/global_resource.hpp>

#include "all_type_variant.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "../plugins/anti_caching/segment_tools.hpp"
#include "../plugins/anti_caching/segment_manager/abstract_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/pmr_segment_manager.hpp"
#include "../plugins/anti_caching/segment_manager/umap_segment_manager.hpp"
#include "benchmark/benchmark.h"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"


namespace opossum {

using namespace anticaching;

// copied from sql benchmark
class StorageAndEncodingTypeBenchmark : public MicroBenchmarkBasicFixture {
 public:

  using T = int32_t;
  inline static const std::string MAPPING_FILE = "test_pool";
  static const size_t MAPPING_FILE_SIZE = 10ul * 1024 * 1024;
  static const ChunkOffset ROW_COUNT = Chunk::DEFAULT_SIZE;

  void SetUp(benchmark::State& st) override {
    // 1. Wir brauchen ein ValueSegment in einer vorgegebenen Größe.
    _value_segment = SegmentTools::create_int_value_segment(ROW_COUNT);
    _sequential_access_pattern = SegmentTools::create_sequential_position_filter(ROW_COUNT);
    _random_access_pattern = SegmentTools::create_random_access_position_filter(ROW_COUNT);
    // 2. Wir brauchen den Typ des Segments, in den encodiert werden soll.
    // 3. Wir brauchen die Speicherart (SegmentManager)
    // 4. Sequentiellen Zugriff messen
    // 5. Randomisierten Zugriff messen
    // we will use iterators
    // 6. Messen abhängig von den Umap parametern
    // 7. Multithreaded messen
  }

  void TearDown(benchmark::State& st) override {
    std::filesystem::remove(MAPPING_FILE);
  }

  void BM_EncodedAccess(benchmark::State& state) {
    for (auto _ : state) {
      auto sum = 0ul;
      auto encoded_segment = SegmentTools::encode_segment(_value_segment, DataType::Int,
                                                          SegmentEncodingSpec{EncodingType::Dictionary});
      auto iterable = create_iterable_from_segment(*_value_segment);
      iterable.for_each(_random_access_pattern, [&](const auto element) {
        benchmark::DoNotOptimize(sum += element.value());
      });
    }
  }

  template<class SegmentType>
  void benchmark_segment_access(benchmark::State& state, const SegmentType& segment,
                                const std::shared_ptr<PosList>& pos_list) {
    for (auto _ : state) {
      auto sum = 0ul;
      auto iterable = create_iterable_from_segment(segment);
      iterable.for_each(pos_list, [&](const auto element) {
        benchmark::DoNotOptimize(sum += element.value());
      });
    }
  }

  void BM_Store(benchmark::State& state) {
    for (auto _ : state) {
      auto segment_manager = UmapSegmentManager(MAPPING_FILE, MAPPING_FILE_SIZE);
      segment_manager.delete_file_on_destruction = false;
      std::shared_ptr<BaseSegment> stored_segment{nullptr};
      benchmark::DoNotOptimize(stored_segment = segment_manager.store(_default_segment_id, *_value_segment));
    }
  }

 protected:
  std::shared_ptr<ValueSegment<T>> _value_segment;
  std::shared_ptr<PosList> _sequential_access_pattern;
  std::shared_ptr<PosList> _random_access_pattern;
  const SegmentID _default_segment_id{"Benchmark", ChunkID{0}, ColumnID{0}, "benchmark"};

};

BENCHMARK_F(StorageAndEncodingTypeBenchmark, BM_Store)(benchmark::State& st) { BM_Store(st); }

BENCHMARK_DEFINE_F(StorageAndEncodingTypeBenchmark, BM_Generic)(benchmark::State& st) {
  const auto encoding_type = encoding_type_enum_values[st.range(0)];
  const auto sequential = st.range(1) == 0;

  // Unencoded, Dictionary, RunLength, FixedStringDictionary, FrameOfReference, LZ4
  std::string label;
  switch (encoding_type) {
    case EncodingType::Unencoded:
      label = "Unencoded";
      break;
    case EncodingType::Dictionary:
      label = "Dictionary";
      break;
    case EncodingType::LZ4:
      label = "LZ4";
      break;
    case EncodingType::RunLength:
      label = "RunLength";
      break;
    case EncodingType::FrameOfReference:
      label = "FrameOfReference";
      break;
    case EncodingType::FixedStringDictionary:
      label = "FixedStringDictionary";
      break;
  }

  if (sequential) label += ", Seq";
  else label += ", Rnd";

  // Segmentgröße festlegen bzw. die Größe der Zugriffsliste? Erst mal egal.
  // Wird das neu generiert?

  // chose relevant value segment
  const auto value_segment = _value_segment;
  // chose relevant position list
  const auto pos_list = sequential ? _sequential_access_pattern : _random_access_pattern;
  // encode segment

  // base_encoded_Segment = ??
  std::shared_ptr<BaseSegment> encoded_segment{value_segment};
  if (encoding_type != EncodingType::Unencoded) {
    encoded_segment = SegmentTools::encode_segment(value_segment, DataType::Int, encoding_type);
  }

  // storage manager festlegen
  std::unique_ptr<AbstractSegmentManager> segment_manager{nullptr};
  switch (st.range(2)) {
    case 0 /* Don't copy segment at all. */:
      label += ", Default allocator";
      break;
    case 1 /* Use pmr_segment_manager with default allocator. */:
      segment_manager = std::make_unique<PmrSegmentManager>(*boost::container::pmr::get_default_resource());
      label += ", PmrSegmentManager(pmr::default_resource)";
      break;
    case 2 /* Use umap_segment_manager */:
      segment_manager = std::make_unique<UmapSegmentManager>(MAPPING_FILE, MAPPING_FILE_SIZE);
      label += ", UmapSegmentManager";
      break;
    default:
    Fail("Unsupported value for st.range(2).");
  }

  // copy segment
  std::shared_ptr<BaseSegment> stored_segment{encoded_segment};
  if (segment_manager) {
    stored_segment = segment_manager->store(_default_segment_id, *encoded_segment);
  }

  // get type of segment.

  // execute benchmark

  st.SetLabel(label);

  if (encoding_type != EncodingType::Unencoded) {
    resolve_encoded_segment_type<T>(*std::static_pointer_cast<BaseEncodedSegment>(stored_segment), [&](const auto& typed_encoded_segment) {
      benchmark_segment_access<decltype(typed_encoded_segment)>(st, typed_encoded_segment, pos_list);
    });
  }
  else {
    benchmark_segment_access<ValueSegment<T>>(st, *std::static_pointer_cast<ValueSegment<T>>(stored_segment), pos_list);
  }

}

static void generic_arguments(benchmark::internal::Benchmark* b) {
  const auto encoding_type_indices = {0, 1, 2, 4, 5};
  // vielleicht geben wir die Chunk size noch mit.
    const auto access_types = {0, 1};
  const auto segment_managers = {0, 1, 2};

  for (const auto encoding_type_index : encoding_type_indices) {
    for (const auto access_type : access_types) {
      for (const auto segment_manager : segment_managers) {
        b->Args({encoding_type_index, access_type, segment_manager});
      }
    }
  }
}

BENCHMARK_REGISTER_F(StorageAndEncodingTypeBenchmark, BM_Generic)->Apply(generic_arguments);

}  // namespace opossum
