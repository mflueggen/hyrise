#pragma once

#include "../jit_types.hpp"
#include "abstract_jittable.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseJitSegmentReaderWrapper;

struct JitInputColumn {
  ColumnID column_id;
  JitTupleEntry tuple_entry;
  bool use_actual_value;
};

struct JitInputLiteral {
  AllTypeVariant value;
  JitTupleEntry tuple_entry;
};

// The JitReadTuples operator only stores the parameters without their actual values. This allows the same JitReadTuples
// operator to be executed with different parameters simultaneously. The JitOperatorWrapper stores the actual values.
struct JitInputParameter {
  ParameterID parameter_id;
  JitTupleEntry tuple_entry;
};

class JitExpression;

// A JitValueIdExpression stores all required information to update a referenced JitExpression to use value ids.
struct JitValueIdExpression {
  std::shared_ptr<JitExpression> jit_expression;

  // Index to the corresponding input column of the left operand
  size_t input_column_index;

  // Index to the corresponding fixed value (i.e. literal or parameter) of the right operand
  std::optional<size_t> input_literal_index;
  std::optional<size_t> input_parameter_index;
};

/* JitReadTuples must be the first operator in any chain of jit operators.
 * It is responsible for:
 * 1) storing literal values to the runtime tuple before the query is executed
 * 2) reading data from the the input table to the runtime tuple
 * 3) advancing the segment iterators
 * 4) keeping track of the number of values in the runtime tuple. Whenever
 *    another operator needs to store a temporary value in the runtime tuple,
 *    it can request a slot in the tuple from JitReadTuples.
 */
class JitReadTuples : public AbstractJittable {
 public:
  explicit JitReadTuples(const bool has_validate = false,
                         const std::shared_ptr<AbstractExpression>& row_count_expression = nullptr);

  std::string description() const final;

  /*
   * The operator code is specialized only once before executing the query. To specialize the code, the operators need
   * to be updated according to the used data encoding. Since this is defined per chunk and not per table, we use the
   * first chunk as a reference for all chunks. If a chunk has a different encoding than the first chunk, it might has
   * to be processed via interpreting the operators as a fallback.
   * The update in the operators includes the change to use value ids in expressions if the corresponding segments are
   * dictionary encoded.
   */
  void before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information);
  /*
   * Prepares the JitRuntimeContext by storing the fixed values (i.e., literals, parameters) in the runtime tuple.
   */
  virtual void before_query(const Table& in_table, const std::vector<AllTypeVariant>& parameter_values,
                            JitRuntimeContext& context) const;
  /*
   * Creates JitSegmentReader instances for the current chunk. Stores relevant chunk data in context.
   * If value ids are used in expressions, the required search value ids from the comparison expressions are looked up
   * in the corresponding dictionary segments and stored in the runtime tuple.
   *
   * @return Indicates whether the specialized function can be used for the current chunk. If the encoding of chunk's
   *         data differs from the encoding of the first chunk (which was used as a reference for specialization), the
   *         specialized function cannot be used for this chunk.
   */
  virtual bool before_chunk(const Table& in_table, const ChunkID chunk_id,
                            const std::vector<AllTypeVariant>& parameter_values, JitRuntimeContext& context);

  /*
   * Methods create a place in the runtime tuple to hold a column, literal, parameter or temporary value which are used
   * by the jittable operators and expressions.
   * The returned JitTupleEntry identifies the position of a value in the runtime tuple.
   */
  JitTupleEntry add_input_column(const DataType data_type, const bool guaranteed_non_null, const ColumnID column_id,
                                 const bool use_actual_value = true);
  JitTupleEntry add_literal_value(const AllTypeVariant& value);
  JitTupleEntry add_parameter(const DataType data_type, const ParameterID parameter_id);
  size_t add_temporary_value();

  /*
   * Adds a JitExpression that can use value ids.
   * The left and right operand of the JitExpression must be added to this JitReadTuples before calling this method.
   */
  void add_value_id_expression(const std::shared_ptr<JitExpression>& jit_expression);

  const std::vector<JitInputColumn>& input_columns() const;
  const std::vector<JitInputLiteral>& input_literals() const;
  const std::vector<JitInputParameter>& input_parameters() const;
  const std::vector<JitValueIdExpression>& value_id_expressions() const;

  std::optional<ColumnID> find_input_column(const JitTupleEntry& tuple_entry) const;
  std::optional<AllTypeVariant> find_literal_value(const JitTupleEntry& tuple_entry) const;

  void execute(JitRuntimeContext& context) const;

  const std::shared_ptr<AbstractExpression> row_count_expression;
  std::vector<std::pair<std::shared_ptr<BaseJitSegmentReaderWrapper>, std::shared_ptr<BaseJitSegmentReaderWrapper>>> _input_wrappers;

 protected:
  uint32_t _num_tuple_values{0};
  std::vector<JitInputColumn> _input_columns;
  std::vector<JitInputLiteral> _input_literals;
  std::vector<JitInputParameter> _input_parameters;
  std::vector<JitValueIdExpression> _value_id_expressions;

 private:
  void _consume(JitRuntimeContext& context) const final {}
  void _create_jit_readers(const Chunk& in_chunk, const std::vector<bool>& segments_are_dictionaries, std::vector<JitReaderContainer>& jit_readers);

  const bool _has_validate;
};

}  // namespace opossum
