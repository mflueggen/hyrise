#include "types.hpp"

namespace opossum::anticaching {

struct SegmentInfo {
  SegmentInfo(SegmentID segment_id, const size_t memory_usage, const ChunkOffset size,
              SegmentAccessCounter::Counter <uint64_t> access_counter)
    : segment_id{std::move(segment_id)}, memory_usage{memory_usage}, size{size},
      access_counter{std::move(access_counter)} {}

  const SegmentID segment_id;
  const size_t memory_usage;
  const ChunkOffset size;
  const SegmentAccessCounter::Counter <uint64_t> access_counter;
};

} // namespace opossum::anticaching
