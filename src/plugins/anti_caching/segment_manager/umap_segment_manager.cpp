#include "umap_segment_manager.hpp"

#include <boost/interprocess/mapped_region.hpp>
#include <sys/mman.h>
#include <umap.h>

namespace opossum::anticaching {

UmapSegmentManager::UmapSegmentManager(const std::string& filename, const size_t file_size)
  : _mmap_memory_resource{filename, file_size} { }

std::shared_ptr<BaseSegment> UmapSegmentManager::store(SegmentID segment_id,
                                                      const BaseSegment& segment) {
  Assert(_active_segments.find(segment_id) == _active_segments.cend(), "Segment with segment_id(" +
    segment_id.to_string() + ") already exists as active segment.");
  Assert(_cached_segments.find(segment_id) == _cached_segments.cend(), "Segment with segment_id(" +
    segment_id.to_string() + ") already exists as cached segment.");

  const auto umap_page_size = umapcfg_get_umap_page_size();
  const auto mmap_position_before_allocation = _mmap_memory_resource.upper_file_pos;

  // todo: refactor, maybe?
  auto copy = segment.copy_using_allocator(&_mmap_memory_resource);
  _cached_segments[segment_id] = copy;
  _active_segments.insert({std::move(segment_id), copy});

  auto mmap_position_after_allocation = _mmap_memory_resource.upper_file_pos;
  // mmap_position_after_allocation has to be divisible bei umap_page_size
  // We round up using some serious bit magic. This works because umap_page_size is a power of 2.
  mmap_position_after_allocation += (umap_page_size - 1);
  mmap_position_after_allocation &= ~(umap_page_size - 1);

  Assert(!msync(_mmap_memory_resource.mmap_pointer() + mmap_position_before_allocation,
                mmap_position_after_allocation - mmap_position_before_allocation, MS_SYNC),
         "msync failed with errno: " + std::to_string(errno));

  // remap using umap
  Assert(umap(_mmap_memory_resource.mmap_pointer(), mmap_position_after_allocation, PROT_READ,
       UMAP_PRIVATE | UMAP_FIXED, _mmap_memory_resource.file_descriptor(), 0) != UMAP_FAILED,
         "Umap failed with errno: " + std::to_string(errno));

  // set upper_file_pos accordingly round up to (page_size + umap page size)
  auto new_upper_file_pos = mmap_position_after_allocation + umap_page_size;
  // umap will actually reserve (mmap_position_after_allocation + umap_page_size), rounded up to umap_page_size, bytes.
  new_upper_file_pos += (umap_page_size - 1);
  new_upper_file_pos &= ~(umap_page_size - 1);

  _mmap_memory_resource.upper_file_pos = new_upper_file_pos;

  return copy;
}

UmapSegmentManager::~UmapSegmentManager() {
  _active_segments.clear();
  _cached_segments.clear();
  if (_mmap_memory_resource.upper_file_pos > 0) {
    Assert(uunmap(_mmap_memory_resource.mmap_pointer(), _mmap_memory_resource.upper_file_pos) >= 0,
           "uunmap failed with errno=" + std::to_string(errno));
  }
  if (delete_file_on_destruction) {
    _mmap_memory_resource.close_and_delete_file();
  }
}

} // opossum::anticaching