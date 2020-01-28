#include "anti_caching_plugin.hpp"

#include <ctime>
#include <iostream>
#include <unordered_map>
#include <numeric>

#include "boost/format.hpp"
#include "hyrise.hpp"
#include "knapsack_solver.hpp"
#include "persistent_memory_manager.hpp"
#include "storage/segment_access_counter.hpp"

namespace opossum {

namespace anticaching {

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

}

AntiCachingPlugin::AntiCachingPlugin() {
  _log_file.open("anti_caching_plugin.log", std::ofstream::app);
  _log_line("Plugin created");
  _memory_resource_handle = PersistentMemoryManager::get().create(4ul * 1024 * 1024 * 1024);
}

AntiCachingPlugin::~AntiCachingPlugin() {
  _log_line("Plugin destroyed");
}

const std::string AntiCachingPlugin::description() const {
  return "AntiCaching Plugin";
}

void AntiCachingPlugin::start() {

  _log_line("Starting Plugin");
  _evaluate_statistics_thread =
    std::make_unique<PausableLoopThread>(REFRESH_STATISTICS_INTERVAL, [&](size_t) { _evaluate_statistics(); });

}

void AntiCachingPlugin::stop() {
  _log_line("Stopping Plugin");
  _evaluate_statistics_thread.reset();
}

void AntiCachingPlugin::_evaluate_statistics() {
  _log_line("Evaluating statistics start");

  const auto timestamp = std::chrono::steady_clock::now();
  // 1. Ermittle alle Segmente // Vielleicht doch nur _fetch_segments?
  // Segment ID und Ptr
  // SegmentID, segment_ptr
  // 2. Übergebe Werte an Knapsack Solver.
  // Bekomme indices zurück, die behalten werden sollen
  // Mappe auf SegmentID. Mache daraus ein set.
  // Iteriere über alle SegmentIDs. Falls in set und nicht evicted. alles gut.
  // Falls in set und evicted -> Hauptspeicher
  // Falls nicht in set und evicted -> alles ok.
  // Falls nicht in set und nicht evicted -> evict.


  auto current_statistics = _fetch_current_statistics();
  if (!current_statistics.empty()) {
    _access_statistics.emplace_back(timestamp, std::move(current_statistics));
  }

  _evict_segments();
  _log_line("Evaluating statistics end");
}

// TODO: Probably not needed?
template <typename Functor>
void AntiCachingPlugin::_for_all_segments(const std::map<std::string, std::shared_ptr<Table>>& tables,
  const Functor& functor) {

  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}, chunk_count = table_ptr->chunk_count(); chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, column_count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < column_count; ++column_id) {
        const auto& column_name = table_ptr->column_name(column_id);
        functor(std::move(SegmentID{table_name, chunk_id, column_id, column_name}),
          std::move(chunk_ptr->get_segment(column_id)));
      }
    }
  }
}

std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> AntiCachingPlugin::_fetch_segments() {
  std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> segments;
  _for_all_segments(Hyrise::get().storage_manager.tables(),
    [&segments](SegmentID segment_id, std::shared_ptr<BaseSegment> segment_ptr) {
    segments.emplace_back(std::move(segment_id), std::move(segment_ptr));
  });
  return segments;
}

std::vector<SegmentInfo> AntiCachingPlugin::_fetch_current_statistics() {
  std::vector<SegmentInfo> access_statistics;
  const auto segments = AntiCachingPlugin::_fetch_segments();
  access_statistics.reserve(segments.size());
  for (const auto& segment_id_segment_ptr_pair : segments) {
    access_statistics.emplace_back(segment_id_segment_ptr_pair.first,
                                   segment_id_segment_ptr_pair.second->memory_usage(MemoryUsageCalculationMode::Sampled),
                                   segment_id_segment_ptr_pair.second->size(),
                                   segment_id_segment_ptr_pair.second->access_counter.counter());
  }
  return access_statistics;
}

void AntiCachingPlugin::_evict_segments() {
  if (_access_statistics.empty()) {
    _log_line("No segments found.");
    return;
  }

  const std::vector<SegmentInfo>& access_statistics = _access_statistics.back().second;
  const std::vector<size_t> indices_to_keep = _determine_memory_segments();

  std::unordered_set<size_t> indices_to_evict;
  for (size_t index = 0, end = access_statistics.size(); index < end; ++index) {
    indices_to_evict.insert(indices_to_evict.end(), index);
  }
  for (const auto index : indices_to_keep) { indices_to_evict.erase(index); }
  DebugAssert(indices_to_evict.size() == access_statistics.size() - indices_to_keep.size(),
              "|indices_to_evict| should be equal to access_statistics.size() - indices_to_keep.size().");

  std::unordered_set<SegmentID, SegmentIDHasher> segment_ids_to_evict;
  for (const auto index : indices_to_evict) {
    segment_ids_to_evict.insert(access_statistics[index].segment_id);
  }

  _swap_segments(segment_ids_to_evict);
}

std::vector<size_t> AntiCachingPlugin::_determine_memory_segments() {
  const std::vector<SegmentInfo>& access_statistics = _access_statistics.back().second;
  std::vector<float> values(access_statistics.size());
  std::vector<size_t> memory_usages(access_statistics.size());

  for (size_t index = 0, end = access_statistics.size(); index < end; ++index) {
    const auto& segment_info = access_statistics[index];
    values[index] = _compute_value(segment_info);
    memory_usages[index] = segment_info.memory_usage;
  }

  auto indices_of_memory_segments = KnapsackSolver::solve(memory_budget, values, memory_usages);

  const auto total_size = std::accumulate(memory_usages.cbegin(), memory_usages.cend(), 0);
  const auto selected_size = std::accumulate(indices_of_memory_segments.cbegin(), indices_of_memory_segments.cend(), 0,
                                             [&memory_usages](const size_t sum, const size_t index) {
                                               return sum + memory_usages[index];
                                             });

  const auto bytes_in_mb = 1024.0f * 1024.0f;
  const auto evicted_memory = ((total_size - selected_size) / bytes_in_mb);
  _log_line((boost::format(
    "%d of %d segments evicted. %f MB of %f MB evicted. %f%% of memory budget used (memory budget: %f MB).") %
             (access_statistics.size() - indices_of_memory_segments.size()) %
             access_statistics.size() %
             evicted_memory %
             (total_size / bytes_in_mb) %
             (100.0f * selected_size / memory_budget) %
             (memory_budget / bytes_in_mb)).str());

  return indices_of_memory_segments;
}

float AntiCachingPlugin::_compute_value(const SegmentInfo& segment_info) {
  // data type spielt vermutlich auch eine Rolle
  const auto& counter = segment_info.access_counter;
  const auto seq_access_factor = 1.0f;
  const auto seq_increasing_access_factor = 1.2f;
  const auto rnd_access_factor = 900'000.0f / 350'000.0f;
  const auto accessor_access_factor = rnd_access_factor;
  const auto dictionary_access_factor = 0.0f;
  // Zugriffe * Zugriffsart // faktor für zugriffsart
  return counter.accessor_access * accessor_access_factor + counter.iterator_seq_access * seq_access_factor +
         counter.iterator_increasing_access + seq_increasing_access_factor +
         counter.iterator_random_access * rnd_access_factor +
         counter.other * rnd_access_factor + counter.dictionary_access * dictionary_access_factor;
}

void AntiCachingPlugin::_swap_segments(const std::unordered_set<SegmentID, SegmentIDHasher>& segment_ids_to_evict) {
  _log_line("swapping segments");
  // TODO: Locking?
  _for_all_segments(Hyrise::get().storage_manager.tables(), [&](const SegmentID segment_id,
                                                                const std::shared_ptr<BaseSegment> segment_ptr) {
    if (segment_ids_to_evict.contains(segment_id)) {
      if (!_evicted_segments.contains(segment_id)) {
        // evict.
        _evicted_segments.insert(segment_id);
        _log_line((boost::format("%s.%s (chunk_id: %d, access_count: %d) evicted.") %
                   segment_id.table_name % segment_id.column_name %
                   segment_id.chunk_id % segment_ptr->access_counter.counter().sum()).str());
      }
    } else {
      // make sure it is in memory
      auto evicted_segment = _evicted_segments.find(segment_id);
      if (evicted_segment != _evicted_segments.cend()) {
        // move into memory
        _evicted_segments.erase(evicted_segment);
        _log_line((boost::format("%s.%s (chunk_id: %d, access_count: %d) moved to memory.") %
                   segment_id.table_name % segment_id.column_name %
                   segment_id.chunk_id % segment_ptr->access_counter.counter().sum()).str());
      }
    }
  });
}

void AntiCachingPlugin::export_access_statistics(const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {
  uint32_t entry_id_counter = 0u;

  std::ofstream meta_file{path_to_meta_data};
  std::ofstream output_file{path_to_access_statistics};
  std::unordered_map<SegmentID, uint32_t, anticaching::SegmentIDHasher> segment_id_entry_id_map;

  meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
  output_file << "entry_id," + SegmentAccessCounter::Counter<uint64_t>::HEADERS + "\n";

  for (const auto& timestamp_segment_info_pair : _access_statistics) {
    const auto& timestamp = timestamp_segment_info_pair.first;
    const auto elapsed_time = timestamp - _initialization_time;
    const auto& segment_infos = timestamp_segment_info_pair.second;
    for (const auto& segment_info : segment_infos) {
      const auto stored_entry_id_it = segment_id_entry_id_map.find(segment_info.segment_id);
      uint32_t entry_id = 0u;
      if (stored_entry_id_it != segment_id_entry_id_map.cend()) entry_id = stored_entry_id_it->second;
      else {
        entry_id = entry_id_counter++;
        // TODO: size and memory_usage are only written once and never updated. This should be changed in the future.
        meta_file << entry_id << ',' << segment_info.segment_id.table_name << ',' << segment_info.segment_id.column_name
                  << ',' << segment_info.segment_id.chunk_id << ',' << segment_info.size << ','
                  << segment_info.memory_usage << '\n';
      }
      output_file << entry_id << ','
                  << std::chrono::duration_cast<std::chrono::seconds>(elapsed_time).count()
                  << segment_info.access_counter.to_string() << '\n';
    }
  }

  meta_file.close();
  output_file.close();
}

void AntiCachingPlugin::_log_line(const std::string& text) {
  const auto timestamp = std::time(nullptr);
  const auto local_time = std::localtime(&timestamp);
  _log_file << std::put_time(local_time, "%d.%m.%Y %H:%M:%S") << ", " << text << '\n';
}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum