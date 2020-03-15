#include "pmr_segment_manager.hpp"

#include "../lib/utils/assert.hpp"


namespace opossum::anticaching {

PmrSegmentManager::PmrSegmentManager(boost::container::pmr::memory_resource& memory_resource)
  : _memory_resource{memory_resource} { }


std::shared_ptr<BaseSegment> PmrSegmentManager::store(SegmentID segment_id,
  const std::shared_ptr<BaseSegment>& segment) {
  Assert(_active_segments.find(segment_id) != _active_segments.cend(), "Segment with segment_id(" +
    segment_id.to_string() + ") already exists as active segment.");
  Assert(_cached_segments.find(segment_id) != _cached_segments.cend(), "Segment with segment_id(" +
    segment_id.to_string() + ") already exists as cached segment.");
  // copy segment using allocator // dem wird die memory resource mitgegeben
  auto copy = segment->copy_using_allocator(_memory_resource);
  _cached_segments[segment_id] = copy;
  _active_segments.insert({std::move(segment_id), copy});
  return copy;
}

} // opossum::anticaching
