#include "jit_utils.hpp"

#include "operators/jit_operator/operators/abstract_jittable.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_segment_reader.hpp"
#include "operators/jit_operator/operators/jit_read_value.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"

namespace opossum {

void place_and_create_jit_reader_wrappers(std::vector<std::shared_ptr<AbstractJittable>>& jit_operators) {
  if (jit_operators.empty()) return;

  auto source = std::dynamic_pointer_cast<JitReadTuples>(jit_operators.front());
  Assert(source, "First jit operator must be JitReadTuples.");

  const auto visit_jit_expressions = [&](const auto& jit_operator, const auto& visitor) {
    const std::function<void(const std::shared_ptr<JitExpression>, const bool, bool)> recursive_iteration =
            [&](const std::shared_ptr<JitExpression> expression, const bool use_value_id, bool execution_not_guaranteed) {
              if (expression->left_child) {
                recursive_iteration(expression->left_child, expression->use_value_ids, execution_not_guaranteed);
              }
              visitor(expression, use_value_id, execution_not_guaranteed);
              if (expression->right_child) {
                execution_not_guaranteed |= expression->expression_type == JitExpressionType::Or || expression->expression_type == JitExpressionType::And;
                recursive_iteration(expression->right_child, expression->use_value_ids, execution_not_guaranteed);
              }
            };

    if (const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operator)) {
      recursive_iteration(jit_filter->expression, false, false);
    } else if (const auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operator)) {
      recursive_iteration(jit_compute->expression, false, false);
    }
  };

  struct MapKey {
    size_t tuple_index;
    bool use_value_id;
    bool operator<(const MapKey& other) const {
      if (tuple_index == other.tuple_index) return use_value_id < other.use_value_id;
      return tuple_index < other.tuple_index;
    }
  };
  struct MapEntry {
    size_t count = 0;
    size_t first_operator_rindex = 0;
    std::shared_ptr<JitExpression> jit_expression = nullptr;
    bool load_is_optional = false;
  };
  std::map<MapKey, MapEntry> access_counter;  // map tuple index to counters
  std::map<size_t, size_t> column_lookup;
  const auto& input_columns = source->input_columns();
  for (size_t input_column_index{0}; input_column_index < input_columns.size(); ++input_column_index) {
    const auto& input_column = input_columns[input_column_index];
    column_lookup[input_column.tuple_entry.tuple_index] = input_column_index;
  }

  std::vector<MapKey> order_by_use;

  for (size_t operator_index{0}; operator_index < jit_operators.size(); ++operator_index) {
    const auto& jit_operator = jit_operators[operator_index];

    const auto count_column_access = [&](const auto& output_columns) {
      for (const auto& column : output_columns) {
        if (column_lookup.count(column.tuple_entry.tuple_index)) {
          MapKey key{column.tuple_entry.tuple_index, false};
          auto& entry = access_counter[key];
          if (!entry.count) {
            entry.first_operator_rindex = jit_operators.size() - operator_index;
            order_by_use.push_back(key);
          }
          ++entry.count;
        }
      }
    };

    if (const auto write_operator = std::dynamic_pointer_cast<JitWriteTuples>(jit_operator)) {
      count_column_access(write_operator->output_columns());
    } else if (const auto aggregate_operator = std::dynamic_pointer_cast<JitAggregate>(jit_operator)) {
      count_column_access(aggregate_operator->groupby_columns());
      count_column_access(aggregate_operator->aggregate_columns());
    } else {
      visit_jit_expressions(jit_operator, [&](const std::shared_ptr<JitExpression>& expression, const bool use_value_id, const bool execution_not_guaranteed) {
        if (!column_lookup.count(expression->result_entry.tuple_index)) return;
        MapKey key{expression->result_entry.tuple_index, use_value_id};
        auto& entry = access_counter[key];
        if (!entry.count) {
          entry.first_operator_rindex = jit_operators.size() - operator_index;
          entry.load_is_optional = execution_not_guaranteed;
          entry.jit_expression = expression;
          order_by_use.push_back(key);
        }
        ++entry.count;
      });
    }
  }

  const auto& wrappers = source->_input_wrappers;

  for (const auto& column : order_by_use) {
    const auto& entry = access_counter[column];
    const auto& column_wrappers = wrappers[column_lookup[column.tuple_index]];
    const auto& correct_wrapper = column.use_value_id ? column_wrappers.second : column_wrappers.first;
    if ((!entry.load_is_optional || entry.count == 1) && entry.jit_expression) {
      correct_wrapper->store_read_value = entry.count > 1;
      entry.jit_expression->segment_read_wrapper = correct_wrapper;
    } else {
      correct_wrapper->store_read_value = true;
      const auto jit_read_value = std::make_shared<JitReadValue>(column.tuple_index, input_columns[column_lookup[column.tuple_index]].column_id, correct_wrapper);
      jit_operators.insert(jit_operators.begin() + (jit_operators.size() - entry.first_operator_rindex), jit_read_value);
    }
  }
}

void connect_jit_operators(std::vector<std::shared_ptr<AbstractJittable>>& jit_operators) {
  for (auto it = jit_operators.begin(); it != jit_operators.end() && it + 1 != jit_operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }
}

}  // namespace opossum
