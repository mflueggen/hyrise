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

#include "anti_caching_config.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "segment_id.hpp"
#include "storage/segment_access_counter.hpp"
#include "segment_info.hpp"
#include "storage/base_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "segment_manager/abstract_segment_manager.hpp"

namespace opossum::anticaching {

class AntiCachingPlugin : public AbstractPlugin {
  friend class AntiCachingPluginTest;

 public:
  AntiCachingPlugin();

  ~AntiCachingPlugin();

  const std::string description() const;

  void start();

  void stop();

  using TimestampSegmentInfosPair = std::pair<const std::chrono::time_point<std::chrono::steady_clock>, std::vector<SegmentInfo>>;

  void export_access_statistics(const std::string& path_to_meta_data, const std::string& path_to_access_statistics) const;
  static void export_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables, const std::string& filename);

  static void reset_access_statistics();

 private:

  static AntiCachingConfig _read_config(const std::string filename);

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

  static uint64_t _sum(const SegmentAccessCounter& counter);



  const std::chrono::time_point<std::chrono::steady_clock> _initialization_time{std::chrono::steady_clock::now()};

  const AntiCachingConfig _config;
  std::ofstream _log_file;
  std::unordered_set<SegmentID, SegmentIDHasher> _evicted_segments;
  std::unordered_map<SegmentID, std::shared_ptr<BaseSegment>, SegmentIDHasher> _persisted_segments;
  std::unique_ptr<AbstractSegmentManager> _segment_manager = nullptr;




  std::vector<TimestampSegmentInfosPair> _access_statistics;
  std::unique_ptr<PausableLoopThread> _evaluate_statistics_thread;


};

}  // namespace opossum::anticaching

