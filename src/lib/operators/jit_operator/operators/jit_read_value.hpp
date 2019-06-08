#pragma once

#include "abstract_jittable.hpp"
#include "jit_read_tuples.hpp"

namespace opossum {

class BaseJitSegmentReaderWrapper;

class JitReadValue : public AbstractJittable {
public:
  explicit JitReadValue(const size_t tuple_index,
                        const ColumnID column_id,
                        std::shared_ptr<BaseJitSegmentReaderWrapper> input_segment_wrapper)
          : _tuple_index(tuple_index),
          _column_id(column_id),
          _input_segment_wrapper(input_segment_wrapper) {}

  std::string description() const final;

private:
  void _consume(JitRuntimeContext& context) const final;

  const size_t _tuple_index;
  const ColumnID _column_id;
  const std::shared_ptr<BaseJitSegmentReaderWrapper> _input_segment_wrapper;
};

}  // namespace opossum