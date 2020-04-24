#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

#include "../plugins/anti_caching/anti_caching_plugin.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "hyrise.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_add_indices.hpp"

#include "third_party/jemalloc/include/jemalloc/jemalloc.h"

using namespace opossum;  // NOLINT

namespace {
const std::unordered_set<std::string> filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }
  return filename_blacklist;
}
}  // namespace

int main(int argc, char* argv[]) {
    bool terminate_thread = false;

  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("TPC-DS Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1 ~ 1GB)", cxxopts::value<int32_t>()->default_value("1"))
    ("enable_breakpoints", "Break at breakpoints", cxxopts::value<bool>()->default_value("false"))
    ("path_to_memory_log", "Path to memory log", cxxopts::value<std::string>()->default_value(""))
    ("path_to_access_statistics_log", "Path to access statistics log", cxxopts::value<std::string>()->default_value(""))
    ("memory_to_lock", "Block of memory to reserve. Not used for anything.", cxxopts::value<uint64_t>()->default_value("0"))
    ("external_setup_file", "Path to external setup file", cxxopts::value<std::string>()->default_value("")); // NOLINT
  // clang-format on

  auto config = std::shared_ptr<opossum::BenchmarkConfig>{};
  auto scale_factor = int32_t{};

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }

      std::thread another_thread([&terminate_thread] {
      size_t allocated_at_start = 1;
      size_t size_of_size_t = sizeof(size_t);
      while (!terminate_thread) {
        mallctl("epoch", nullptr, nullptr, &allocated_at_start, size_of_size_t);
        if (mallctl("stats.allocated", &allocated_at_start, &size_of_size_t, nullptr, 0)) {
          std::cout << "failed" << "\n";
        }
        std::cout << allocated_at_start << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      }
    });

//  const auto enable_breakpoints = cli_parse_result["enable_breakpoints"].as<bool>();
  const auto path_to_memory_log = cli_parse_result["path_to_memory_log"].as<std::string>();
  const auto path_to_access_statistics_log = cli_parse_result["path_to_access_statistics_log"].as<std::string>();
  const auto external_setup_file = cli_parse_result["external_setup_file"].as<std::string>();
//  const auto memory_to_lock = cli_parse_result["memory_to_lock"].as<uint64_t>();

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

  scale_factor = cli_parse_result["scale"].as<int32_t>();

  config = std::make_shared<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_cli_options(cli_parse_result));

  const auto valid_scale_factors = std::array{1, 1000, 3000, 10000, 30000, 100000};

  const auto& find_result = std::find(valid_scale_factors.begin(), valid_scale_factors.end(), scale_factor);
  Assert(find_result != valid_scale_factors.end(),
         "TPC-DS benchmark only supports scale factor 1 (qualification only), 1000, 3000, 10000, 30000 and 100000.");

  std::cout << "- TPC-DS scale factor is " << scale_factor << std::endl;

  std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

  Assert(std::filesystem::is_directory(query_path), "Query path (" + query_path + ") has to be a directory.");
  Assert(std::filesystem::exists(std::filesystem::path{query_path + "/01.sql"}), "Queries have to be available.");

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
  if (config->verify) {
    query_generator->load_dedicated_expected_results(
        std::filesystem::path{"resources/benchmark/tpcds/tpcds-result-reproduction/answer_sets_tbl"});
  }
  auto table_generator = std::make_unique<TpcdsTableGenerator>(scale_factor, config);
  auto benchmark_runner = BenchmarkRunner{*config, std::move(query_generator), std::move(table_generator),
                                          opossum::BenchmarkRunner::create_context(*config)};
  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpcds/schema.sql", "resources/benchmark/tpcds/create_indices.sql",
                          benchmark_runner.sqlite_wrapper);
  }
  std::cout << "done." << std::endl;


  benchmark_runner.run();

  if (!path_to_access_statistics_log.empty()) {
    anticaching::AntiCachingPlugin::export_access_statistics(Hyrise::get().storage_manager.tables(),
                                                             path_to_access_statistics_log);
  }

  terminate_thread = true;
  another_thread.join();
  logging.join();
}
