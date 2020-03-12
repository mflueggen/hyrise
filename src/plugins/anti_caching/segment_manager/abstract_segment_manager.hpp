#include "types.hpp"

#include <unordered_map>

#include "../segment_id.hpp"
#include "storage/base_segment.hpp"

namespace opossum::anticaching {

class AbstractSegmentManager: private Noncopyable {
 public:
  explicit AbstractSegmentManager();
  virtual ~AbstractSegmentManager() = default;

  std::shared_ptr<BaseSegment> load(const SegmentID& segment_id);
  std::shared_ptr<BaseSegment> store(SegmentID segment_id, std::shared_ptr<BaseSegment> segment);
  bool remove(const SegmentID& segment_id);

 protected:
  std::unordered_map<SegmentID, std::shared_ptr<BaseSegment>, SegmentIDHasher> _cached_segments;
  std::unordered_map<SegmentID, std::shared_ptr<BaseSegment>, SegmentIDHasher> _active_segments;
};

} // namespace opossum::anticaching