#pragma once

#include <memory>

#include "../segment_id.hpp"
#include "abstract_segment_manager.hpp"
#include "boost/container/pmr/memory_resource.hpp"
#include "storage/base_segment.hpp"

namespace opossum::anticaching {

class PmrSegmentManager: public AbstractSegmentManager {
 public:
  PmrSegmentManager(boost::container::pmr::memory_resource& memory_resource);
  std::shared_ptr<BaseSegment> store(SegmentID segment_id, const BaseSegment& segment) override;

 private:
  boost::container::pmr::memory_resource& _memory_resource;
};

} // namespace opossum::anticaching