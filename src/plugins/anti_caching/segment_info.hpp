#pragma once

#include "../lib/storage/segment_access_counter.hpp"
#include "segment_id.hpp"
#include "types.hpp"

namespace opossum::anticaching {

struct SegmentInfo {
  SegmentInfo(SegmentID segment_id, const size_t memory_usage, const ChunkOffset size,
              SegmentAccessCounter access_counter);

  const SegmentID segment_id;
  const size_t memory_usage;
  const ChunkOffset size;
  const SegmentAccessCounter access_counter;
};

} // namespace opossum::anticaching
