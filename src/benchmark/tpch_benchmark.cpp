#include <sys/mman.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "../plugins/anti_caching/anti_caching_plugin.hpp"
#include "../plugins/anti_caching/segment_manager/lockable_segment_manager.hpp"
#include "../third_party/nlohmann_json/single_include/nlohmann/json.hpp"
#include "SQLParserResult.h"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "hyrise.hpp"
#include "storage/segment_access_counter.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_add_indices.hpp"

#include "third_party/jemalloc/include/jemalloc/jemalloc.h"
#include "../plugins/anti_caching/segment_id.hpp"

using namespace opossum;  // NOLINT

class cgroup_info {
 public:
  size_t rss;
  size_t hierarchical_memory_limit;
  size_t unevictable;
  std::string cgroup;

  cgroup_info(size_t rss, size_t hierarchical_memory_limit, size_t unevictable, const std::string& cgroup)
      : rss{rss}, hierarchical_memory_limit{hierarchical_memory_limit}, unevictable{unevictable}, cgroup{cgroup} {}

  void refresh() {
    const auto filename = "/sys/fs/cgroup/memory/" + cgroup + "/memory.stat";
    std::ifstream file(filename);
    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        // look for "rss [0-9]+"
        if (line.starts_with("rss ")) {
          auto size_as_string = line.substr(4, line.length() - 4);
          this->rss = std::strtoul(size_as_string.c_str(), nullptr, 10);
        }
        else if (line.starts_with("hierarchical_memory_limit ")) {
          auto size_as_string = line.substr(26, line.length() - 26);
          this->hierarchical_memory_limit = std::strtoul(size_as_string.c_str(), nullptr, 10);
        }
        else if (line.starts_with("unevictable ")) {
          auto size_as_string = line.substr(12, line.length() - 12);
          this->unevictable = std::strtoul(size_as_string.c_str(), nullptr, 10);
        }
      }
      file.close();
    }
    else {
      std::cout << "cgroup_info::load Opening " << filename << " failed.\n";
      exit(EXIT_FAILURE);
    }
  }

  static cgroup_info from_cgroup(const std::string& cgroup) {
    cgroup_info info = cgroup_info{0, 0, 0, cgroup};
    info.refresh();
    return info;
  }
};

class jemalloc_info {
 public:
  size_t allocated;
  size_t resident;

  jemalloc_info(size_t allocated, size_t resident)
      : allocated{allocated}, resident{resident} {}

  void refresh(bool purge = true) {
    size_t size_of_size_t = sizeof(size_t);

    if (purge) mallctl("arena.4096.purge", NULL, NULL, NULL, 0);

    mallctl("epoch", nullptr, nullptr, &this->allocated, size_of_size_t);

    mallctl("stats.allocated", &this->allocated, &size_of_size_t, nullptr, 0);
    mallctl("stats.resident", &this->resident, &size_of_size_t, nullptr, 0);
  }

  static jemalloc_info get(bool purge = true) {
    auto jemalloc_data = jemalloc_info(0, 0);
    jemalloc_data.refresh(purge);
    return jemalloc_data;
  }
};

void breakpoint(const std::string& message) {
  const auto info = jemalloc_info::get();
  std::cout << message << "\n";
  std::cout << "stats.allocated=" << info.allocated << '\n';
  std::cout << "stats.resident=" << info.resident << '\n';
  std::cout << "Press ENTER to continue...\n";
  std::getchar();
}

void initialize_lockable_segment_manager(anticaching::LockableSegmentManager& lockable_segment_manager) {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto chunk_count = table->chunk_count();
    const auto column_count = table->column_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto segment = chunk->get_segment(column_id);
        auto copy = lockable_segment_manager.store(anticaching::SegmentID(table_name, chunk_id, column_id, table->column_name(column_id)), *segment);
        chunk->replace_segment(column_id, copy);
      }
    }
  }
}

void apply_locking(const std::string& filename, anticaching::LockableSegmentManager& lockable_segment_manager, bool unlock) {
  std::ifstream config_file(filename);
  nlohmann::json json_config;
  config_file >> json_config;
  const auto type = json_config.value("type", "locked");

  for (const auto& memory_segment: json_config["memory_segments"].items()) {
    const ChunkID chunk_id = ChunkID{memory_segment.value()["chunk_id"]};
    const std::string table_name = memory_segment.value()["table_name"];
    const std::string column_name = memory_segment.value()["column_name"];

    auto table = Hyrise::get().storage_manager.get_table(table_name);
    const auto column_id = table->column_id_by_name(column_name);
    const auto segment_id = anticaching::SegmentID{table_name, chunk_id, column_id, column_name};

    if (unlock) {
      // apply dont need
      lockable_segment_manager.unlock(segment_id);
    }
    else {
      lockable_segment_manager.lock(segment_id);
    }
  }
}

void limit_free_memory(size_t free_memory_limit, const std::string& cgroup) {
  auto jemalloc_data = jemalloc_info::get();
  auto cgroup_data = cgroup_info::from_cgroup(cgroup);

  const auto lock_block_size = 512ul*1024*1024;
  auto memory_to_lock = cgroup_data.hierarchical_memory_limit - cgroup_data.unevictable - free_memory_limit;

  std::cout << "Imposing free memory limit:"
            << "\nfree_memory_limit = " << free_memory_limit
            << "\nstats.allocated = " << jemalloc_data.allocated
            << "\nstats.resident = " << jemalloc_data.resident
            << "\ncgroup.rss = " << cgroup_data.rss
            << "\ncgroup.hierarchical_memory_limit = " << cgroup_data.hierarchical_memory_limit
            << "\ncgroup.unevictable = " << cgroup_data.unevictable
            << "\nmemory_to_lock = " << memory_to_lock << "\n";

  // this does not work...
//  char* locked_memory = (char*)malloc(memory_to_lock);
//  if (mlock(locked_memory, memory_to_lock)) {
//    std::cout << "mlock failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
//    exit(-1);
//  }

    while(cgroup_data.hierarchical_memory_limit - cgroup_data.unevictable > free_memory_limit) {
      char* locked_memory = (char*)malloc(lock_block_size);
      if (mlock(locked_memory, lock_block_size)) {
        std::cout << "mlock failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
        exit(-1);
      }
      jemalloc_data.refresh();
      cgroup_data.refresh();
    }


//  for (auto i = 0ul; i < space_to_lock;  i+=lock_block_size) {
//    char* locked_memory = (char*)malloc(lock_block_size);
//    if (mlock(locked_memory, lock_block_size)) {
//      std::cout << "mlock failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
//      exit(-1);
//    }
//    mallctl("arena.4096.purge", NULL, NULL, NULL, 0);
//  }

}

/**
 * This benchmark measures Hyrise's performance executing the TPC-H *queries*, it doesn't (yet) support running the
 * TPC-H *benchmark* exactly as it is specified.
 * (Among other things, the TPC-H requires performing data refreshes and has strict requirements for the number of
 * sessions running in parallel. See http://www.tpc.org/tpch/default.asp for more info)
 * The benchmark offers a wide range of options (scale_factor, chunk_size, ...) but most notably it offers two modes:
 * IndividualQueries and PermutedQuerySets. See docs on BenchmarkMode for details.
 * The benchmark will stop issuing new queries if either enough iterations have taken place or enough time has passed.
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-H Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("q,queries", "Specify queries to run (comma-separated query ids, e.g. \"--queries 1,3,19\"), default is all", cxxopts::value<std::string>()) // NOLINT
    ("use_prepared_statements", "Use prepared statements instead of random SQL strings", cxxopts::value<bool>()->default_value("false"))
    ("enable_breakpoints", "Break at breakpoints", cxxopts::value<bool>()->default_value("false"))
    ("mlockall", "Call mlockall before query start", cxxopts::value<bool>()->default_value("false"))
    ("init_lockable_segment_manager", "Move all segments to lockable segment manager", cxxopts::value<bool>()->default_value("false"))
    ("path_to_memory_log", "Path to memory log", cxxopts::value<std::string>()->default_value(""))
    ("path_to_access_statistics_log", "Path to access statistics log", cxxopts::value<std::string>()->default_value(""))
    ("memory_to_lock", "Block of memory to reserve. Not used for anything.", cxxopts::value<uint64_t>()->default_value("0"))
    ("cgroup", "Name of cgroup restricting total memory", cxxopts::value<std::string>()->default_value(""))
    ("free_memory_limit", "Free memory, that should be available for executing the TPCH Benchmark."
     "Must be used together with a cgroup", cxxopts::value<uint64_t>()->default_value("0"))
    ("unlock_segments", "Path to file", cxxopts::value<std::string>()->default_value(""))
    ("lock_segments", "Path to file", cxxopts::value<std::string>()->default_value("")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> config;
  std::string comma_separated_queries;
  float scale_factor;
  bool use_prepared_statements;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  const auto enable_breakpoints = cli_parse_result["enable_breakpoints"].as<bool>();
  if (enable_breakpoints) breakpoint("After parsing command line args.");

  const auto mlock_all = cli_parse_result["mlockall"].as<bool>();
  const auto init_lockable_segment_manager = cli_parse_result["init_lockable_segment_manager"].as<bool>();
  const auto path_to_memory_log = cli_parse_result["path_to_memory_log"].as<std::string>();
  const auto path_to_access_statistics_log = cli_parse_result["path_to_access_statistics_log"].as<std::string>();
  const auto unlock_segment_path = cli_parse_result["unlock_segments"].as<std::string>();
  const auto lock_segment_path = cli_parse_result["lock_segments"].as<std::string>();
  const auto memory_to_lock = cli_parse_result["memory_to_lock"].as<uint64_t>();
  const auto cgroup = cli_parse_result["cgroup"].as<std::string>();
  const auto free_memory_limit = cli_parse_result["free_memory_limit"].as<uint64_t>();

  if ((!lock_segment_path.empty() || !unlock_segment_path.empty()) && !init_lockable_segment_manager) {
    Fail("lock_segment_path and unlock_segment_path must be used together with init_lockable_segment_manager");
  }

  if (free_memory_limit > 0 && cgroup.empty()) {
    Fail("If a free memory limit is imposed a cgroup name must be given.");
  }

  // spin up thread to measure memory consumption
  bool terminate_thread = false;
  std::thread logging([&terminate_thread, &path_to_memory_log]{
    if (path_to_memory_log.empty())
      return;
    std::ofstream output_file{path_to_memory_log};
    jemalloc_info jemalloc_data{0, 0};
    while(!terminate_thread) {
      jemalloc_data.refresh();
      output_file << jemalloc_data.allocated;
      output_file << "," << jemalloc_data.resident << "\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    output_file.close();
  });

//  std::thread another_thread([&terminate_thread]{
//    size_t size_of_size_t = sizeof(size_t);
//    while(!terminate_thread) {
//      size_t epoch = 1;
//      size_t allocated_at_start = 0;
//      mallctl("epoch", &epoch, &size_of_size_t, &epoch, size_of_size_t);
//      if (mallctl("stats.allocated", &allocated_at_start, &size_of_size_t, nullptr, 0)) {
//        std::cout << "failed" << "\n";
//      }
//      std::cout << allocated_at_start << "\n";
//      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//    }
//  });

  {
    if (cli_parse_result.count("queries")) {
      comma_separated_queries = cli_parse_result["queries"].as<std::string>();
    }

    scale_factor = cli_parse_result["scale"].as<float>();

    config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

    use_prepared_statements = cli_parse_result["use_prepared_statements"].as<bool>();

    std::vector<BenchmarkItemID> item_ids;

    // Build list of query ids to be benchmarked and display it
    if (comma_separated_queries.empty()) {
      std::transform(tpch_queries.begin(), tpch_queries.end(), std::back_inserter(item_ids),
                     [](auto& pair) { return BenchmarkItemID{pair.first - 1}; });
    } else {
      // Split the input into query ids, ignoring leading, trailing, or duplicate commas
      auto item_ids_str = std::vector<std::string>();
      boost::trim_if(comma_separated_queries, boost::is_any_of(","));
      boost::split(item_ids_str, comma_separated_queries, boost::is_any_of(","), boost::token_compress_on);
      std::transform(item_ids_str.begin(), item_ids_str.end(), std::back_inserter(item_ids),
                     [](const auto& item_id_str) {
                       const auto item_id =
                         BenchmarkItemID{boost::lexical_cast<BenchmarkItemID::base_type, std::string>(item_id_str) - 1};
                       DebugAssert(item_id < 22, "There are only 22 TPC-H queries");
                       return item_id;
                     });
    }

    std::cout << "- Benchmarking Queries: [ ";
    auto printable_item_ids = std::vector<std::string>();
    std::for_each(item_ids.begin(), item_ids.end(),
                  [&printable_item_ids](auto& id) { printable_item_ids.push_back(std::to_string(id + 1)); });
    std::cout << boost::algorithm::join(printable_item_ids, ", ") << " ]" << std::endl;

    auto context = BenchmarkRunner::create_context(*config);

    Assert(!use_prepared_statements || !config->verify, "SQLite validation does not work with prepared statements");

    if (config->verify) {
      // Hack: We cannot verify TPC-H Q15, thus we remove it from the list of queries
      auto it = std::remove(item_ids.begin(), item_ids.end(), 15 - 1);
      if (it != item_ids.end()) {
        // The problem is that the last part of the query, "DROP VIEW", does not return a table. Since we also have
        // the TPC-H test against a known-to-be-good table, we do not want the additional complexity for handling this
        // in the BenchmarkRunner.
        std::cout << "- Skipping Query 15 because it cannot easily be verified" << std::endl;
        item_ids.erase(it, item_ids.end());
      }
    }

    std::cout << "- TPCH scale factor is " << scale_factor << std::endl;
    std::cout << "- Using prepared statements: " << (use_prepared_statements ? "yes" : "no") << std::endl;

    // Add TPCH-specific information
    context.emplace("scale_factor", scale_factor);
    context.emplace("use_prepared_statements", use_prepared_statements);

    auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor,
                                                                 item_ids);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config), context);
    Hyrise::get().benchmark_runner = benchmark_runner;

    if (config->verify) {
      add_indices_to_sqlite("resources/benchmark/tpch/schema.sql", "resources/benchmark/tpch/indices.sql",
                            benchmark_runner->sqlite_wrapper);
    }

    auto lockable_segment_manager = anticaching::LockableSegmentManager();
    if (init_lockable_segment_manager) {
      if (enable_breakpoints) breakpoint("Before initialize_lockable_segment_manager.");
      initialize_lockable_segment_manager(lockable_segment_manager);
      if (enable_breakpoints) breakpoint("After initialize_lockable_segment_manager.");
    }

    if (mlock_all) {
      if (enable_breakpoints) breakpoint("Before mlockall");
      if (mlockall(MCL_CURRENT)) {
        std::cout << "mlockall failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
        exit(-1);
      }
      if (enable_breakpoints) breakpoint("After mlockall");
    }

    // unlocked memory should not add to the total memory
    auto relocking_required = !unlock_segment_path.empty();
    if (relocking_required) {
      if (enable_breakpoints) breakpoint("Before unlocking segments");
      apply_locking(unlock_segment_path, lockable_segment_manager, true);
      if (enable_breakpoints) breakpoint("After unlocking segments");
    }

    if (free_memory_limit > 0) {
      if (enable_breakpoints) breakpoint("Before limit free memory");
      limit_free_memory(free_memory_limit, cgroup);
      if (enable_breakpoints) breakpoint("After limit free memory");
    }

    if (memory_to_lock > 0) {
      if (enable_breakpoints) breakpoint("Before reserving " + std::to_string(memory_to_lock) + " bytes");

      auto* locked_memory = malloc(memory_to_lock);
      if (!locked_memory) {
        std::cout << "malloc failed.\n";
        exit(-1);
      }

      if (mlock(locked_memory, memory_to_lock)) {
        std::cout << "mlock failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
        exit(-1);
      }

      if (enable_breakpoints) breakpoint("After reserving " + std::to_string(memory_to_lock) + " bytes");
    }

    // lock: locked memory should reduce free mem.
    relocking_required = !lock_segment_path.empty();
    if (relocking_required) {
      if (enable_breakpoints) breakpoint("Before locking segments");
      apply_locking(lock_segment_path, lockable_segment_manager, false);
      if (enable_breakpoints) breakpoint("After locking segments");
    }

    if (enable_breakpoints) breakpoint("benchmark_runner->run()");
    benchmark_runner->run();
    if (enable_breakpoints) breakpoint("After benchmark_runner->run()");

    if (!path_to_access_statistics_log.empty()) {
      anticaching::AntiCachingPlugin::export_access_statistics(Hyrise::get().storage_manager.tables(),
                                                               path_to_access_statistics_log);
    }

    if (enable_breakpoints) {
      breakpoint("Before benchmark cleanup");
    }
  }

  if (enable_breakpoints) {
    Hyrise::get().benchmark_runner.reset();
    breakpoint("after benchmark cleanup");
  }

  terminate_thread = true;
  logging.join();
//  another_thread.join();

  if (enable_breakpoints) breakpoint("Before leaving main().");
}
