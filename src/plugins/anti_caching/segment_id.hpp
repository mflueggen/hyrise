#pragma once

#include <string>

#include "types.hpp"

namespace opossum::anticaching {

struct SegmentID {
  SegmentID(const std::string& table_name, const ChunkID chunk_id, const ColumnID column_id,
            const std::string& column_name);

  std::string table_name;
  ChunkID chunk_id;
  ColumnID column_id;
  std::string column_name;

  bool operator==(const SegmentID& other) const;

  std::string to_string() const;
};

struct SegmentIDHasher {
  std::size_t operator()(const SegmentID& segment_id) const;
};

} // namespace opossum::anticaching
