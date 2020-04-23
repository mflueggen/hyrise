#include <sys/mman.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "../plugins/anti_caching/anti_caching_plugin.hpp"
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

using namespace opossum;  // NOLINT


void break_point(const std::string& message) {
  size_t allocated_at_start;
  size_t size_of_size_t = sizeof(size_t);
  std::cout << message << "\n";
  mallctl("epoch", nullptr, nullptr, &allocated_at_start, size_of_size_t);
  mallctl("stats.allocated", &allocated_at_start, &size_of_size_t, nullptr, 0);
  std::cout << "stats.allocated=" << allocated_at_start << '\n';
  std::cout << "Press ENTER to continue...\n";
  std::getchar();
}

void external_setup(const std::string& filename) {
  std::ifstream config_file(filename);
  nlohmann::json json_config;
  config_file >> json_config;
  const auto type = json_config.value("type", "locked");

  for (const auto& memory_segment: json_config["memory_segments"].items()) {
    std::cout << memory_segment.key() << "\n";
    std::cout << memory_segment.value() << "\n";

    // hier segmente auslesen und abhängig vom typ entsprechend verarbeiten.
  }
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
    ("path_to_memory_log", "Path to memory log", cxxopts::value<std::string>()->default_value(""))
    ("path_to_access_statistics_log", "Path to access statistics log", cxxopts::value<std::string>()->default_value(""))
    ("memory_to_lock", "Memory to lock", cxxopts::value<uint64_t>()->default_value("0"))
    ("external_setup_file", "Path to external setup file", cxxopts::value<std::string>()->default_value("")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> config;
  std::string comma_separated_queries;
  float scale_factor;
  bool use_prepared_statements;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  const auto enable_breakpoints = cli_parse_result["enable_breakpoints"].as<bool>();
  if (enable_breakpoints) break_point("After parsing command line args.");

  const auto path_to_memory_log = cli_parse_result["path_to_memory_log"].as<std::string>();
  const auto path_to_access_statistics_log = cli_parse_result["path_to_access_statistics_log"].as<std::string>();
  const auto external_setup_file = cli_parse_result["external_setup_file"].as<std::string>();
  const auto memory_to_lock = cli_parse_result["memory_to_lock"].as<uint64_t>();

  if (!external_setup_file.empty()) {
    external_setup(external_setup_file);
  }

  // spin up thread to measure memory consumption
  bool terminate_thread = false;
  std::thread logging([&terminate_thread, &path_to_memory_log]{
    if (path_to_memory_log.empty())
      return;
    std::ofstream output_file{path_to_memory_log};
    size_t allocated_at_start;
    size_t size_of_size_t = sizeof(size_t);
    while(!terminate_thread) {
      mallctl("epoch", nullptr, nullptr, &allocated_at_start, size_of_size_t);
      mallctl("stats.allocated", &allocated_at_start, &size_of_size_t, nullptr, 0);
      output_file << allocated_at_start << "\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    output_file.close();
  });

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
    std::transform(item_ids_str.begin(), item_ids_str.end(), std::back_inserter(item_ids), [](const auto& item_id_str) {
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

  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor, item_ids);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config), context);
  Hyrise::get().benchmark_runner = benchmark_runner;

  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpch/schema.sql", "resources/benchmark/tpch/indices.sql",
                          benchmark_runner->sqlite_wrapper);
  }

  if (enable_breakpoints) break_point("Before reserving " + std::to_string(memory_to_lock) + " bytes");

  if (memory_to_lock > 0) {
    const auto LOCKED_MEMORY_SIZE = 4ul * 1024 * 1024 * 1024;
    auto* locked_memory = malloc(LOCKED_MEMORY_SIZE);
    if (!locked_memory) {
      std::cout << "malloc failed.\n";
      exit(-1);
    }

    if (mlock(locked_memory, LOCKED_MEMORY_SIZE)) {
      std::cout << "mlock failed with error '" << std::strerror(errno) << "' (" << errno << ")" << "\n";
      exit(-1);
    }
  }

  if (enable_breakpoints) break_point("After reserving " + std::to_string(memory_to_lock) + " bytes");

  if (!external_setup_file.empty()) {
    external_setup(external_setup_file);
  }

  if (enable_breakpoints) break_point("After external preparation.");

  benchmark_runner->run();

  if (!path_to_access_statistics_log.empty()) {
    anticaching::AntiCachingPlugin::export_access_statistics(Hyrise::get().storage_manager.tables(),
                                                             path_to_access_statistics_log);
  }

  terminate_thread = true;
  logging.join();
  if (enable_breakpoints) break_point("Before leaving main().");
}
