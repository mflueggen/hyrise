#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/pos_lists/rowid_pos_list.hpp"
#include "storage/value_segment.hpp"


namespace opossum {

class SegmentTools {
 public:
  static const size_t DEFAULT_ROW_COUNT = 1024ul;
  static const size_t MAX_VALUE = 1'024ul;

  static size_t row_count(EncodingType encoding_type);

  static std::shared_ptr<ValueSegment<int32_t>> create_int_value_segment(size_t row_count);


  static std::shared_ptr<ValueSegment<int32_t>> create_int_with_null_value_segment(size_t row_count);


  static std::shared_ptr<RowIDPosList> create_sequential_position_filter(size_t row_count);


  static std::shared_ptr<RowIDPosList> create_random_access_position_filter(size_t row_count);

  static std::shared_ptr<BaseEncodedSegment> encode_segment(const std::shared_ptr<BaseSegment>& base_segment,
                                                            const DataType data_type,
                                                            const SegmentEncodingSpec& segment_encoding_spec);

  static void export_access_statistics();
};

} // namespace opossum
