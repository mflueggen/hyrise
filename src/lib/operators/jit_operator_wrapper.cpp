#include <operators/jit_operator/operators/jit_write_tuples.hpp>
#include "jit_operator_wrapper.hpp"

#include "expression/expression_utils.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_segment_reader.hpp"
#include "operators/jit_operator/operators/jit_read_value.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(const std::shared_ptr<const AbstractOperator>& left,
                                       const JitExecutionMode execution_mode,
                                       const std::shared_ptr<SpecializedFunctionWrapper>& specialized_function_wrapper)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left},
      _execution_mode{execution_mode},
      _specialized_function_wrapper{specialized_function_wrapper} {}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _specialized_function_wrapper->jit_operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) {
  _specialized_function_wrapper->jit_operators.push_back(op);
}

const std::vector<std::shared_ptr<AbstractJittable>>& JitOperatorWrapper::jit_operators() const {
  return _specialized_function_wrapper->jit_operators;
}

const std::vector<AllTypeVariant>& JitOperatorWrapper::input_parameter_values() const {
  return _input_parameter_values;
}

const std::shared_ptr<JitReadTuples> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuples>(_specialized_function_wrapper->jit_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_specialized_function_wrapper->jit_operators.back());
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  Assert(_source(), "JitOperatorWrapper does not have a valid source node.");
  Assert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  _prepare_and_specialize_operator_pipeline();

  const auto in_table = input_left()->get_output();

  auto out_table = _sink()->create_output_table(*in_table);

  JitRuntimeContext context;
  if (transaction_context_is_set()) {
    context.transaction_id = transaction_context()->transaction_id();
    context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
  }

  _source()->before_query(*in_table, _input_parameter_values, context);
  _sink()->before_query(*out_table, context);

  for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count() && context.limit_rows; ++chunk_id) {
    bool use_specialized_function = _source()->before_chunk(*in_table, chunk_id, _input_parameter_values, context);
    if (use_specialized_function) {
      _specialized_function_wrapper->execute_func(_source().get(), context);
    } else {
      _source()->execute(context);
    }
    _sink()->after_chunk(in_table, *out_table, context);
  }

  _sink()->after_query(*out_table, context);

  return out_table;
}

void JitOperatorWrapper::_prepare_and_specialize_operator_pipeline() {
  // Use a mutex to specialize a jittable operator pipeline within a subquery only once.
  // See jit_operator_wrapper.hpp for details.
  std::lock_guard<std::mutex> guard(_specialized_function_wrapper->specialization_mutex);
  if (_specialized_function_wrapper->execute_func) return;

  const auto in_table = input_left()->get_output();

  if (in_table->chunk_count() == 0) return;

  const auto& jit_operators = _specialized_function_wrapper->jit_operators;

  std::vector<bool> tuple_non_nullable_information;
  for (auto& jit_operator : jit_operators) {
    jit_operator->before_specialization(*in_table, tuple_non_nullable_information);
  }

  _insert_jit_reader_wrappers();

  // Connect operators to a chain
  for (auto it = jit_operators.begin(); it != jit_operators.end() && it + 1 != jit_operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  std::function<void(const JitReadTuples*, JitRuntimeContext&)> execute_func;
  // We want to perform two specialization passes if the operator chain contains a JitAggregate operator, since the
  // JitAggregate operator contains multiple loops that need unrolling.
  auto two_specialization_passes = static_cast<bool>(std::dynamic_pointer_cast<JitAggregate>(_sink()));
  switch (_execution_mode) {
    case JitExecutionMode::Compile:
      // this corresponds to "opossum::JitReadTuples::execute(opossum::JitRuntimeContext&) const"
      _specialized_function_wrapper->execute_func =
          _specialized_function_wrapper->module
              .specialize_and_compile_function<void(const JitReadTuples*, JitRuntimeContext&)>(
                  "_ZNK7opossum13JitReadTuples7executeERNS_17JitRuntimeContextE",
                  std::make_shared<JitConstantRuntimePointer>(_source().get()), two_specialization_passes);
      break;
    case JitExecutionMode::Interpret:
      _specialized_function_wrapper->execute_func = &JitReadTuples::execute;
      break;
  }
}

void JitOperatorWrapper::_insert_jit_reader_wrappers() {
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
  const auto& input_columns = _source()->input_columns();
  for (size_t input_column_index{0}; input_column_index < input_columns.size(); ++input_column_index) {
    const auto& input_column = input_columns[input_column_index];
    column_lookup[input_column.tuple_entry.tuple_index] = input_column_index;
  }

  auto& jit_operators = _specialized_function_wrapper->jit_operators;

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
        if (expression->expression_type != JitExpressionType::Column) return;
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

  const auto& wrappers = _source()->_input_wrappers;

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

void JitOperatorWrapper::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  const auto& input_parameters = _source()->input_parameters();
  _input_parameter_values.resize(input_parameters.size());

  for (size_t index{0}; index < input_parameters.size(); ++index) {
    const auto search = parameters.find(input_parameters[index].parameter_id);
    if (search != parameters.end()) {
      _input_parameter_values[index] = search->second;
    }
  }

  // Set any parameter values used within in the row count expression.
  if (const auto row_count_expression = _source()->row_count_expression) {
    expression_set_parameters(row_count_expression, parameters);
  }
}

void JitOperatorWrapper::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  // Set the MVCC data in the row count expression required by possible subqueries within the expression.
  if (const auto row_count_expression = _source()->row_count_expression) {
    expression_set_transaction_context(row_count_expression, transaction_context);
  }
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JitOperatorWrapper>(copied_input_left, _execution_mode, _specialized_function_wrapper);
}

}  // namespace opossum
