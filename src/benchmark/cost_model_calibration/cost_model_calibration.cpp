#include <algorithm>
#include <iostream>
#include <fstream>
#include <json.hpp>

#include "cost_model_calibration.hpp"
#include "cost_model_feature_extractor.hpp"
#include "query/calibration_query_generator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_query_cache.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "utils/format_duration.hpp"
#include "utils/load_table.hpp"


namespace opossum {

CostModelCalibration::CostModelCalibration(const CalibrationConfiguration configuration): _configuration(configuration) {
  const auto table_specifications = configuration.table_specifications;

  for (const auto& table_specification : table_specifications) {
    std::cout << "Loading table " << table_specification.table_name << std::endl;
    auto table = load_table(table_specification.table_path, 100000);
    std::cout << "Loaded table " << table_specification.table_name << " successfully." << std::endl;

    ChunkEncodingSpec chunk_spec;

    for (const auto& column_specification : table_specification.columns) {
      auto column = column_specification.second;
      chunk_spec.push_back(column.encoding);
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    StorageManager::get().add_table(table_specification.table_name, table);

    std::cout << "Encoded table " << table_specification.table_name << " successfully." << std::endl;
  }
}

void CostModelCalibration::calibrate() {
  auto number_of_iterations = _configuration.calibration_runs;

  const auto scheduler = std::make_shared<NodeQueueScheduler>();
  CurrentScheduler::set(scheduler);

  for (size_t i = 0; i < number_of_iterations; i++) {
    // Regenerate Queries for each iteration...
    auto queries = CalibrationQueryGenerator::generate_queries(_configuration.table_specifications);

    for (const auto& query : queries) {
      std::cout << "Running " << query << std::endl;
      SQLQueryCache<SQLQueryPlan>::get().clear();

      auto pipeline_builder = SQLPipelineBuilder{query};
      pipeline_builder.disable_mvcc();
      pipeline_builder.dont_cleanup_temporaries();
      auto pipeline = pipeline_builder.create_pipeline();

      // Execute the query, we don't care about the results
      pipeline.get_result_table();

      auto query_plans = pipeline.get_query_plans();
      for (const auto & query_plan : query_plans) {
        for (const auto& root : query_plan->tree_roots()) {
          _traverse(root);
        }
      }
    }
    std::cout << "Finished iteration " << i << std::endl;
  }

  auto outputPath = _configuration.output_path;

  nlohmann::json output_json{};
  output_json["config"] = _configuration;
  output_json["operators"] = _operators;

  // output file per operator type
  std::ofstream myfile;
  myfile.open(outputPath);
  myfile << std::setw(2) << output_json << std::endl;
  myfile.close();
}

void CostModelCalibration::_traverse(const std::shared_ptr<const AbstractOperator> & op) {
  auto description = op->name();
  auto operator_result = CostModelFeatureExtractor::extract_features(op);
  _operators[description].push_back(operator_result);

  if (op->input_left() != nullptr) {
    _traverse(op->input_left());
  }

  if (op->input_right() != nullptr) {
    _traverse(op->input_right());
  }
}

}  // namespace opossum