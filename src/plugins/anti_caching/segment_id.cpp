#include "segment_id.hpp"

namespace opossum::anticaching {

SegmentID::SegmentID(const std::string& table_name, const ChunkID chunk_id, const ColumnID column_id,
                     const std::string& column_name)
  : table_name{table_name}, chunk_id{chunk_id}, column_id{column_id}, column_name{column_name} {}

bool SegmentID::operator==(const SegmentID& other) const {
  return table_name == other.table_name && chunk_id == other.chunk_id && column_id == other.column_id;
}

size_t SegmentIDHasher::operator()(const SegmentID& segment_id) const {
  size_t res = 17;
  res = res * 31 + std::hash<std::string>()(segment_id.table_name);
  res = res * 31 + std::hash<ChunkID>()(segment_id.chunk_id);
  res = res * 31 + std::hash<ColumnID>()(segment_id.column_id);
  return res;
}

} // namespace opossum::anticaching