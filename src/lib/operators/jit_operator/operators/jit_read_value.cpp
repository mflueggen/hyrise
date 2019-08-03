#include "jit_read_value.hpp"

#include "constant_mappings.hpp"
#include "jit_segment_reader.hpp"

namespace opossum {

std::string JitReadValue::description() const {
  std::stringstream desc;
  desc << "[ReadValue] x" << _tuple_index << " = Col#" << _column_id << ", ";
  return desc.str();
}

void JitReadValue::_consume(JitRuntimeContext& context) const {
  _input_segment_wrapper->read_and_store_value(context);
  _emit(context);
}

}  // namespace opossum