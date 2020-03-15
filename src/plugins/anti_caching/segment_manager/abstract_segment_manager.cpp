#include "abstract_segment_manager.hpp"

namespace opossum::anticaching {

std::shared_ptr<BaseSegment> AbstractSegmentManager::load(const SegmentID& segment_id) {
  auto segment_it = _active_segments.find(segment_id);
  if (segment_it != _active_segments.cend()) return segment_it->second;
  segment_it = _cached_segments.find(segment_id);
  if (segment_it != _cached_segments.cend()) {
    _active_segments[segment_id] = segment_it->second;
    return segment_it->second;
  }
  return std::shared_ptr<BaseSegment>{nullptr};
}

bool AbstractSegmentManager::remove(const SegmentID& segment_id) {
  auto segment_it = _active_segments.find(segment_id);
  if (segment_it == _active_segments.cend()) return false;
  _active_segments.erase(segment_it);
  return true;
}

} // opossum::anticaching
