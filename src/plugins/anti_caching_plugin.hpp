#pragma once

#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <sstream>
#include <string>

#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "storage/base_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

namespace anticaching {
struct SegmentID {
  SegmentID(const std::string& table_name, const ChunkID chunk_id, const ColumnID column_id,
            const std::string& column_name) : table_name{table_name}, chunk_id{chunk_id}, column_id{column_id},
                                              column_name{column_name} {}

  std::string table_name;
  ChunkID chunk_id;
  ColumnID column_id;
  std::string column_name;

  bool operator==(const SegmentID& other) const;
};

struct SegmentIDHasher {
  std::size_t operator()(const SegmentID& segment_id) const;
};

struct SegmentInfo {
  SegmentInfo(SegmentID segment_id, const size_t memory_usage, const ChunkOffset size,
              SegmentAccessCounter::Counter<uint64_t> access_counter)
    : segment_id{std::move(segment_id)}, memory_usage{memory_usage}, size{size},
      access_counter{std::move(access_counter)} {}

  const SegmentID segment_id;
  const size_t memory_usage;
  const ChunkOffset size;
  const SegmentAccessCounter::Counter<uint64_t> access_counter;
};
}

using namespace anticaching;

class AntiCachingPlugin : public AbstractPlugin {
  friend class AntiCachingPluginTest;

 public:
  AntiCachingPlugin();

  ~AntiCachingPlugin();

  const std::string description() const;

  void start();

  void stop();

  using TimestampSegmentInfosPair = std::pair<const std::chrono::time_point<std::chrono::steady_clock>, std::vector<SegmentInfo>>;

  void export_access_statistics(const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  void reset_access_statistics();

  size_t memory_budget = 5ul * 1024ul * 1024ul;

 private:

  template<typename Functor>
  static void _for_all_segments(const std::map<std::string, std::shared_ptr<Table>>& tables,
                                bool include_mutable_chunks, const Functor& functor);

  std::vector<SegmentInfo> _fetch_current_statistics();

  static std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> _fetch_segments();

  void _evaluate_statistics();

  void _evict_segments();

  std::vector<SegmentID> _determine_in_memory_segments();

  static std::vector<SegmentInfo>
  _select_segment_information_for_value_computation(const std::vector<TimestampSegmentInfosPair>& access_statistics);

  static float _compute_value(const SegmentInfo& segment_info);

  void _swap_segments(const std::vector<SegmentID>& in_memory_segment_ids);

  void _log_line(const std::string& text);

  std::ofstream _log_file;
  size_t _memory_resource_handle;
  std::unordered_set<SegmentID, SegmentIDHasher> _evicted_segments;
  std::unordered_map<SegmentID, std::shared_ptr<BaseSegment>, SegmentIDHasher> _persisted_segments;

  std::vector<TimestampSegmentInfosPair> _access_statistics;
  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;

  constexpr static std::chrono::milliseconds REFRESH_STATISTICS_INTERVAL = std::chrono::milliseconds(10'000);
  const std::chrono::time_point<std::chrono::steady_clock> _initialization_time{std::chrono::steady_clock::now()};
};

}  // namespace opossum

