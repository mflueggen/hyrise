#include "lockable_segment_manager.hpp"

#include "types.hpp"

namespace opossum::anticaching {

std::shared_ptr<BaseSegment> LockableSegmentManager::store(SegmentID segment_id, const BaseSegment& segment) {
  Assert(_active_segments.find(segment_id) == _active_segments.cend(), "Segment with segment_id(" +
                                                                       segment_id.to_string() + ") already exists as active segment.");
  Assert(_cached_segments.find(segment_id) == _cached_segments.cend(), "Segment with segment_id(" +
                                                                       segment_id.to_string() + ") already exists as cached segment.");

  // round up to page size
  auto memory_usage = segment.memory_usage(MemoryUsageCalculationMode::Full) + LockableMemoryResource::PAGE_SIZE - 1;
  memory_usage &= ~(LockableMemoryResource::PAGE_SIZE - 1);

  auto lockable_resource = std::make_unique<LockableMemoryResource>(memory_usage);
  auto copy = segment.copy_using_allocator(&(*lockable_resource));
  _lockable_resources[segment_id] = std::move(lockable_resource);
  _cached_segments[segment_id] = copy;
  _active_segments.insert({std::move(segment_id), copy});

  return copy;
}

LockableSegmentManager::~LockableSegmentManager() {
  _cached_segments.clear();
  _active_segments.clear();
}

void LockableSegmentManager::lock(const SegmentID& segment_id) {
  _lockable_resources[segment_id]->lock();
}

void LockableSegmentManager::unlock(const SegmentID& segment_id) {
  _lockable_resources[segment_id]->unlock();
}

}  // namespace opossum::anticaching
