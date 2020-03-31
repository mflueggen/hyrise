#pragma once

#include "../lib/storage/segment_access_counter.hpp"
#include "segment_id.hpp"
#include "types.hpp"

namespace opossum::anticaching {

enum class SegmentType {Value, Dictionary, LZ4, FrameOfReference, RunLength};

struct SegmentInfo {
  SegmentInfo(SegmentID segment_id, const size_t memory_usage, const ChunkOffset size, const SegmentType type,
              SegmentAccessCounter access_counter);

  const SegmentID segment_id;
  const size_t memory_usage;
  const ChunkOffset size;
  const SegmentType type;
  const SegmentAccessCounter access_counter;
  bool in_memory = true;
};

struct SegmentMetaData {
  const size_t memory_usage;
  const ChunkOffset size;
  const SegmentType type;
};

} // namespace opossum::anticaching
