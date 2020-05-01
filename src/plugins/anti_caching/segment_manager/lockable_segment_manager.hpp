#pragma once

#include <memory>
#include <unordered_map>

#include "../segment_id.hpp"
#include "abstract_segment_manager.hpp"
#include "boost/container/pmr/memory_resource.hpp"
#include "storage/base_segment.hpp"
#include "../memory_resource/lockable_memory_resource.hpp"

namespace opossum::anticaching {

class LockableSegmentManager : public AbstractSegmentManager {
 public:
  ~LockableSegmentManager() override;

  void lock(const SegmentID& segment_id);

  void unlock(const SegmentID& segment_id);

  std::shared_ptr<BaseSegment> store(SegmentID segment_id, const BaseSegment& segment) override;

 private:
  std::unordered_map<SegmentID, std::unique_ptr<LockableMemoryResource>, SegmentIDHasher> _lockable_resources;


};

}  // namespace opossum::anticaching
