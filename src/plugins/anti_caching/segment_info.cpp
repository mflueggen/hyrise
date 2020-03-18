#include "segment_info.hpp"

namespace opossum::anticaching {

SegmentInfo::SegmentInfo(SegmentID segment_id, const size_t memory_usage, const ChunkOffset size,
                         SegmentAccessCounter access_counter)
  : segment_id{std::move(segment_id)}, memory_usage{memory_usage}, size{size},
    access_counter{std::move(access_counter)} {}

} // opossum::anticaching
