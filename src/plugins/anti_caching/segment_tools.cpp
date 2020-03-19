#include "segment_tools.hpp"

#include <random>

#include "storage/chunk_encoder.hpp"
#include "storage/frame_of_reference_segment.hpp"


namespace opossum {

size_t SegmentTools::row_count(EncodingType encoding_type) {
  switch (encoding_type) {
    case EncodingType::FrameOfReference:
      // fill three blocks and a bit more
      return static_cast<size_t>(FrameOfReferenceSegment<int32_t>::block_size * (3.3));
    default:
      return DEFAULT_ROW_COUNT;
  }
}

std::shared_ptr<ValueSegment<int32_t>> SegmentTools::create_int_value_segment(size_t row_count) {
  auto values = pmr_vector<int32_t>(row_count);

  std::default_random_engine engine{};
  std::uniform_int_distribution<int32_t> dist{0u, MAX_VALUE};

  for (auto& elem : values) {
    elem = dist(engine);
  }

  return std::make_shared<ValueSegment<int32_t>>(std::move(values));
}

std::shared_ptr<ValueSegment<int32_t>> SegmentTools::create_int_with_null_value_segment(size_t row_count) {
  auto values = pmr_vector<int32_t>(row_count);
  auto null_values = pmr_vector<bool>(row_count);

  std::default_random_engine engine{};
  std::uniform_int_distribution<int32_t> dist{0u, MAX_VALUE};
  std::bernoulli_distribution bernoulli_dist{0.3};

  for (auto i = 0u; i < row_count; ++i) {
    values[i] = dist(engine);
    null_values[i] = bernoulli_dist(engine);
  }

  return std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
}

std::shared_ptr<PosList> SegmentTools::create_sequential_position_filter(size_t row_count) {
  auto list = std::make_shared<PosList>();
  list->guarantee_single_chunk();

  std::default_random_engine engine{};
  std::bernoulli_distribution bernoulli_dist{0.5};

  for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < row_count; ++offset_in_referenced_chunk) {
    if (bernoulli_dist(engine)) {
      list->push_back(RowID{ChunkID{0}, offset_in_referenced_chunk});
    }
  }

  return list;
}

std::shared_ptr<PosList> SegmentTools::create_random_access_position_filter(size_t row_count) {
  auto list = create_sequential_position_filter(row_count);

  auto random_device = std::random_device{};
  std::default_random_engine engine{random_device()};
  std::shuffle(list->begin(), list->end(), engine);

  return list;
}

std::shared_ptr<BaseEncodedSegment> SegmentTools::encode_segment(const std::shared_ptr<BaseSegment>& base_segment,
                                                   const DataType data_type,
                                                   const SegmentEncodingSpec& segment_encoding_spec) {
  return std::dynamic_pointer_cast<BaseEncodedSegment>(
    ChunkEncoder::encode_segment(base_segment, data_type, segment_encoding_spec));
}

} // namespace opossum