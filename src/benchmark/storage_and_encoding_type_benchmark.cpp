#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "../plugins/anti_caching/segment_tools.hpp"
#include "benchmark/benchmark.h"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

// copied from sql benchmark
class StorageAndEncodingTypeBenchmark : public MicroBenchmarkBasicFixture {
 public:

  using T = int32_t;

  void SetUp(benchmark::State& st) override {
    const auto ROW_COUNT = Chunk::DEFAULT_SIZE;
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

  void BM_SequentialAccess(benchmark::State& state) {
    for (auto _ : state) {
      auto sum = 0ul;
      auto iterable = create_iterable_from_segment(*_value_segment);
      iterable.for_each(_sequential_access_pattern, [&](const auto element) {
        benchmark::DoNotOptimize(sum += element.value());
      });
    }
  }

  void BM_RandomAccess(benchmark::State& state) {
    for (auto _ : state) {
      auto sum = 0ul;
      auto iterable = create_iterable_from_segment(*_value_segment);
      iterable.for_each(_random_access_pattern, [&](const auto element) {
        benchmark::DoNotOptimize(sum += element.value());
      });
    }
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

  void BM_Generic(benchmark::State& state, const EncodingType encoding_type, const bool sequential) {
    const auto pos_list = sequential ? _sequential_access_pattern : _random_access_pattern;
    if (encoding_type == EncodingType::Unencoded) {
      benchmark_segment_access<decltype(*_value_segment)>(state, *_value_segment, pos_list);
      return;
    }

    auto base_encoded_segment = SegmentTools::encode_segment(_value_segment, DataType::Int, encoding_type);
    resolve_encoded_segment_type<T>(*base_encoded_segment, [&](const auto& encoded_segment) {
      benchmark_segment_access<decltype(encoded_segment)>(state, encoded_segment, pos_list);
    });
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

 protected:
  std::shared_ptr<ValueSegment<T>> _value_segment;
  std::shared_ptr<PosList> _sequential_access_pattern;
  std::shared_ptr<PosList> _random_access_pattern;

};

//BENCHMARK_F(StorageAndEncodingTypeBenchmark, BM_SequentialAccess)(benchmark::State& st) { BM_SequentialAccess(st); }
//
//BENCHMARK_F(StorageAndEncodingTypeBenchmark, BM_RandomAccess)(benchmark::State& st) { BM_RandomAccess(st); }
//
//BENCHMARK_F(StorageAndEncodingTypeBenchmark, BM_EncodedAccess)(benchmark::State& st) { BM_EncodedAccess(st); }

//BENCHMARK_F(StorageAndEncodingTypeBenchmark, BM_Generic)(benchmark::State& st) { BM_Generic(st, EncodingType::Unencoded, true); }

BENCHMARK_DEFINE_F(StorageAndEncodingTypeBenchmark, BM_Generic)(benchmark::State& st) {
  const auto encoding_type = encoding_type_enum_values[st.range(0)];
  const auto sequential = st.range(1) == 0;

  // Unencoded, Dictionary, RunLength, FixedStringDictionary, FrameOfReference, LZ4
  std::string label;
  switch (encoding_type) {
    case EncodingType::Unencoded: label = "Unencoded"; break;
    case EncodingType::Dictionary: label = "Dictionary"; break;
    case EncodingType::LZ4: label = "LZ4"; break;
    case EncodingType::RunLength: label = "RunLength"; break;
    case EncodingType::FrameOfReference: label = "FrameOfReference"; break;
    case EncodingType::FixedStringDictionary: label = "FixedStringDictionary"; break;
  }

  if (sequential) label += ", Seq";
  else label += ", Rnd";

  st.SetLabel(label);
  BM_Generic(st, encoding_type, sequential);
}

static void generic_arguments(benchmark::internal::Benchmark* b) {
  const auto encoding_type_indices = {0, 1, 2, 4, 5};
  const auto access_types = {0, 1};
  for (const auto encoding_type_index : encoding_type_indices) {
    for (const auto access_type : access_types) {
      b->Args({encoding_type_index, access_type});
    }
  }
}

BENCHMARK_REGISTER_F(StorageAndEncodingTypeBenchmark, BM_Generic)->Apply(generic_arguments);

}  // namespace opossum
