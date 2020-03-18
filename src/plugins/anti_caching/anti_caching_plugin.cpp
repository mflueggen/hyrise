#include "anti_caching_plugin.hpp"

#include <ctime>

#include <iostream>
#include <numeric>

#include "boost/format.hpp"
#include "hyrise.hpp"
#include "knapsack_solver.hpp"
#include "persistent_memory_manager.hpp"
#include "storage/segment_access_counter.hpp"
#include "third_party/jemalloc/include/jemalloc/jemalloc.h"


namespace opossum::anticaching {

AntiCachingPlugin::AntiCachingPlugin()
  : _config{_read_config("anti_caching_plugin.json")} {
  _log_file.open("anti_caching_plugin.log", std::ofstream::app);
  if (_config.memory_resource_type == AntiCachingConfig::PMEM_MEMORY_RESOURCE_TYPE) {
    _memory_resource_handle = PersistentMemoryManager::get().create_pmemobj(_config.pool_size);
  }
  else {
    _memory_resource_handle = PersistentMemoryManager::get().create_mmap(_config.pool_size);
  }
  _log_line("Plugin created with " + _config.to_string());
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
    std::make_unique<PausableLoopThread>(std::chrono::milliseconds(_config.segment_eviction_interval_in_ms),
                                         [&](size_t) { _evaluate_statistics(); });
}

void AntiCachingPlugin::stop() {
  _log_line("Stopping Plugin");
  _evaluate_statistics_thread.reset();
}

AntiCachingConfig AntiCachingPlugin::_read_config(const std::string filename) {
  return AntiCachingConfig(filename);
}

void AntiCachingPlugin::_evaluate_statistics() {
  _log_line("Evaluating statistics start");

  const auto timestamp = std::chrono::steady_clock::now();

  auto current_statistics = _fetch_current_statistics();
  if (!current_statistics.empty()) {
    _access_statistics.emplace_back(timestamp, std::move(current_statistics));
  }

  _evict_segments();
  _log_line("Evaluating statistics end");
}

// TODO: Probably not needed?
template<typename Functor>
void AntiCachingPlugin::_for_all_segments(const std::map<std::string, std::shared_ptr<Table>>& tables,
                                          bool include_mutable_chunks, const Functor& functor) {

  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}, chunk_count = table_ptr->chunk_count(); chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk_ptr = table_ptr->get_chunk(chunk_id);
      if (!include_mutable_chunks && chunk_ptr->is_mutable()) continue;
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
  _for_all_segments(Hyrise::get().storage_manager.tables(), false,
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
                                   segment_id_segment_ptr_pair.second->memory_usage(
                                     MemoryUsageCalculationMode::Sampled),
                                   segment_id_segment_ptr_pair.second->size(),
                                   segment_id_segment_ptr_pair.second->access_counter);
  }
  return access_statistics;
}

void AntiCachingPlugin::_evict_segments() {
  if (_access_statistics.empty()) {
    _log_line("No segments found.");
    return;
  }

  const auto in_memory_segments = _determine_in_memory_segments();
  _swap_segments(in_memory_segments);
}

/**
 * Determines the indices of segments (in _access_statistics) that shall remain in memory.
 * @return returns set of segment ids?
 */
std::vector<SegmentID> AntiCachingPlugin::_determine_in_memory_segments() {
  const auto segment_information = _select_segment_information_for_value_computation(_access_statistics);

  // Segment ID, counters
  std::vector<float> values(segment_information.size());
  std::vector<size_t> memory_usages(segment_information.size());

  for (size_t index = 0, end = segment_information.size(); index < end; ++index) {
    const auto& segment_info = segment_information[index];
    values[index] = _compute_value(segment_info);
    memory_usages[index] = segment_info.memory_usage;
  }

  auto indices_of_memory_segments = KnapsackSolver::solve(_config.memory_budget, values, memory_usages);

  std::vector<SegmentID> segment_ids;
  segment_ids.reserve(indices_of_memory_segments.size());

  for (const auto index : indices_of_memory_segments) {
    segment_ids.push_back(segment_information[index].segment_id);
  }

  const auto total_size = std::accumulate(memory_usages.cbegin(), memory_usages.cend(), 0);
  const auto selected_size = std::accumulate(indices_of_memory_segments.cbegin(), indices_of_memory_segments.cend(), 0,
                                             [&memory_usages](const size_t sum, const size_t index) {
                                               return sum + memory_usages[index];
                                             });

  const auto bytes_in_mb = 1024.0f * 1024.0f;
  const auto evicted_memory = ((total_size - selected_size) / bytes_in_mb);
  _log_line((boost::format(
    "%d of %d segments evicted. %f MB of %f MB evicted. %f%% of memory budget used (memory budget: %f MB).") %
             (segment_information.size() - indices_of_memory_segments.size()) %
             segment_information.size() %
             evicted_memory %
             (total_size / bytes_in_mb) %
             (100.0f * selected_size / _config.memory_budget) %
             (_config.memory_budget / bytes_in_mb)).str());

  return segment_ids;
}

std::vector<SegmentInfo>
AntiCachingPlugin::_select_segment_information_for_value_computation(
  const std::vector<TimestampSegmentInfosPair>& access_statistics) {
  if (access_statistics.empty()) return {};
  const auto& current = access_statistics.back().second;
  if (access_statistics.size() == 1) return current;

  const auto& prev = (access_statistics.cend() - 2)->second;

  std::unordered_map<SegmentID, SegmentInfo, SegmentIDHasher> segment_infos;

  for (const auto& segment_info : prev) {
    segment_infos.insert({segment_info.segment_id, segment_info});
  }

  std::vector<SegmentInfo> return_vector;
  return_vector.reserve(current.size());

  for (const auto& segment_info : current) {
    const auto& prev_segment_info = segment_infos.find(segment_info.segment_id);
    if (prev_segment_info != segment_infos.cend()) {
//      return_vector.emplace_back(segment_info.segment_id, segment_info.memory_usage, segment_info.size,
//                                 segment_info.access_counter - prev_segment_info->second.access_counter);
    } else {
      return_vector.push_back(segment_info);
    }
  }
  return return_vector;
}

float AntiCachingPlugin::_compute_value(const SegmentInfo& segment_info) {
  // data type spielt vermutlich auch eine Rolle
//  const auto& counter = segment_info.access_counter;
//  const auto seq_access_factor = 1.0f;
//  const auto seq_increasing_access_factor = 1.2f;
//  const auto rnd_access_factor = 900'000.0f / 350'000.0f;
//  const auto accessor_access_factor = rnd_access_factor;
//  const auto dictionary_access_factor = 0.0f;
  // Zugriffe * Zugriffsart // faktor für zugriffsart
//  return counter.accessor_access * accessor_access_factor + counter.iterator_seq_access * seq_access_factor +
//         counter.iterator_increasing_access + seq_increasing_access_factor +
//         counter.iterator_random_access * rnd_access_factor +
//         counter.other * rnd_access_factor + counter.dictionary_access * dictionary_access_factor;
  return 0;
}

/**
 *
 * @param in_memory_segment_ids Ids of segments which should be copied to main memory. All other ids are moved to a
 * secondary memory.
 */
void AntiCachingPlugin::_swap_segments(const std::vector<SegmentID>& in_memory_segment_ids) {
  _log_line("swapping segments");

  std::unordered_set<SegmentID, SegmentIDHasher> in_memory_segment_ids_set;
  for (const auto& segment_id : in_memory_segment_ids) {
    in_memory_segment_ids_set.insert(segment_id);
  }

  // Dieser Code ist abhängig von der verwendeten MemoryResource
  // Für UMAP benötigen benötigen wir eine eigene Resource zum Schreiben der Daten.
  // Wegschreiben mit mmap
  // Ich muss immer Blockweise speicher reservieren.
  // ggf. neue mmap datei anlegen
  // Segmente verschieben
  // msync
  // mmap Datei schließen
  // Datei mit umap öffnen und geöffnet halten
  // Copy Segment using allocator mit umap pointer.
  // Diese bleiben alle gemappt.
  // Kann ich mmap



  auto bytes_evicted = 0ul;
  auto bytes_restored = 0ul;
  size_t allocated_at_start;
  size_t size_of_size_t = sizeof(size_t);
  mallctl("epoch", nullptr, nullptr, &allocated_at_start, size_of_size_t);
  mallctl("stats.allocated", &allocated_at_start, &size_of_size_t, nullptr, 0);

  // TODO: We need a write and read handle
  // Switch to
  auto& persistent_memory_resource = PersistentMemoryManager::get().get(_memory_resource_handle);
  // TODO: Locking?
  _for_all_segments(Hyrise::get().storage_manager.tables(), false, [&](const SegmentID segment_id,
                                                                       const std::shared_ptr<BaseSegment> segment_ptr) {

    // copy segments from secondary memory to main memory
    if (in_memory_segment_ids_set.contains(segment_id)) {
      // make sure it is in memory
      auto evicted_segment = _evicted_segments.find(segment_id);
      if (evicted_segment != _evicted_segments.cend()) {
        // move into memory
        auto copy_of_segment = segment_ptr->copy_using_allocator({});
        auto table_ptr = Hyrise::get().storage_manager.get_table(segment_id.table_name);
        auto chunk_ptr = table_ptr->get_chunk(segment_id.chunk_id);
        chunk_ptr->replace_segment(segment_id.column_id, copy_of_segment);
        // TODO: This is just for logging purposes
        auto segment_size = segment_ptr->memory_usage(MemoryUsageCalculationMode::Full);
        _evicted_segments.erase(evicted_segment);
        _log_line((boost::format("%s.%s (chunk_id: %d, access_count: %d, size: %d) moved to memory.") %
                   segment_id.table_name % segment_id.column_name %
                   // TODO: FIX
                   0 % //segment_id.chunk_id % segment_ptr->access_counter.counter().sum() %
                   segment_size).str());
        bytes_restored += segment_size;
      }
    } else {
      // evict segment.
      // copy segments from main memory to secondary memory.
      if (!_evicted_segments.contains(segment_id)) {
        // ggf. Kopie anlegen
        auto copy_of_segment = std::shared_ptr<BaseSegment>{nullptr};
        auto persisted_segment_it = _persisted_segments.find(segment_id);
        if (persisted_segment_it != _persisted_segments.cend()) {
          copy_of_segment = (*persisted_segment_it).second;
        } else {
          copy_of_segment = segment_ptr->copy_using_allocator(&persistent_memory_resource);
          _persisted_segments.insert(persisted_segment_it, {segment_id, copy_of_segment});
          _log_line((boost::format("%s.%s (chunk_id: %d) persisted.") %
                     segment_id.table_name % segment_id.column_name %
                     segment_id.chunk_id).str());
        }
        auto table_ptr = Hyrise::get().storage_manager.get_table(segment_id.table_name);
        auto chunk_ptr = table_ptr->get_chunk(segment_id.chunk_id);
        // man müsste diese Informationen wegspeichern
        // die mmap datei schließen
        // Remap mit umap
        // Frage ist, ob ein Pointer erhalten bleibt, wenn ich das mapping schließe und wieder öffne.
        // Vermutlich kann man sich nicht darauf verlassen.
        // benötigen wir die offsets
        chunk_ptr->replace_segment(segment_id.column_id, copy_of_segment);

        auto segment_size = segment_ptr->memory_usage(MemoryUsageCalculationMode::Full);
        _evicted_segments.insert(segment_id);
        _log_line((boost::format("%s.%s (chunk_id: %d, access_count: %d, size: %d) evicted.") %
                   segment_id.table_name % segment_id.column_name %
                   // TODO: FIX
                   0 % //segment_id.chunk_id % segment_ptr->access_counter.counter().sum() %
                   segment_size).str());
        bytes_evicted += segment_size;
      }
      // an dieser Stelle dontneed setzen.
      // damit das funktioniert, muss ich den Speicherbereich für das gesamte Segment kennen.
    }
  });

  size_t allocated_at_end;
  mallctl("epoch", nullptr, nullptr, &allocated_at_end, size_of_size_t);
  mallctl("stats.allocated", &allocated_at_end, &size_of_size_t, nullptr, 0);

  _log_line((boost::format("Segments swapped. Bytes evicted: %d, Bytes restored: %d, stats.allocated start: %d, "
                           "stats.allocated end: %d, stats.allocated diff: %s%d.") %
             bytes_evicted % bytes_restored % allocated_at_start % allocated_at_end %
             (allocated_at_end < allocated_at_start ? "-" : "+") %
             (std::max(allocated_at_start, allocated_at_end) - std::min(allocated_at_start, allocated_at_end))).str());
}

void AntiCachingPlugin::export_access_statistics(const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {
//  uint32_t entry_id_counter = 0u;

//  std::ofstream meta_file{path_to_meta_data};
//  std::ofstream output_file{path_to_access_statistics};
//  std::unordered_map<SegmentID, uint32_t, anticaching::SegmentIDHasher> segment_id_entry_id_map;

//  meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
//  output_file << "entry_id," + SegmentAccessCounter::Counter<uint64_t>::HEADERS + "\n";
//
//  for (const auto& timestamp_segment_info_pair : _access_statistics) {
//    const auto& timestamp = timestamp_segment_info_pair.first;
//    const auto elapsed_time = timestamp - _initialization_time;
//    const auto& segment_infos = timestamp_segment_info_pair.second;
//    for (const auto& segment_info : segment_infos) {
//      const auto stored_entry_id_it = segment_id_entry_id_map.find(segment_info.segment_id);
//      uint32_t entry_id = 0u;
//      if (stored_entry_id_it != segment_id_entry_id_map.cend()) entry_id = stored_entry_id_it->second;
//      else {
//        entry_id = entry_id_counter++;
//        // TODO: size and memory_usage are only written once and never updated. This should be changed in the future.
//        meta_file << entry_id << ',' << segment_info.segment_id.table_name << ',' << segment_info.segment_id.column_name
//                  << ',' << segment_info.segment_id.chunk_id << ',' << segment_info.size << ','
//                  << segment_info.memory_usage << '\n';
//      }
//      output_file << entry_id << ','
//                  << std::chrono::duration_cast<std::chrono::seconds>(elapsed_time).count()
//                  << segment_info.access_counter.to_string() << '\n';
//    }
//  }

//  meta_file.close();
//  output_file.close();
}

void AntiCachingPlugin::_log_line(const std::string& text) {
  const auto timestamp = std::time(nullptr);
  const auto local_time = std::localtime(&timestamp);
  _log_file << std::put_time(local_time, "%d.%m.%Y %H:%M:%S") << ", " << text << '\n';
  _log_file.flush();
}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum