#pragma once

#include <memory>
#include <string>

#include "../segment_id.hpp"
#include "../memory_resource/mmap_memory_resource.hpp"
#include "abstract_segment_manager.hpp"
#include "storage/base_segment.hpp"

namespace opossum::anticaching {

class UmapSegmentManager: public AbstractSegmentManager {
 public:
  UmapSegmentManager(const std::string& filename, const size_t file_size);
  std::shared_ptr<BaseSegment> store(SegmentID segment_id, const std::shared_ptr<BaseSegment>& segment) override;

 private:
  MmapMemoryResource _mmap_memory_resource;
};

} // namespace opossum::anticaching