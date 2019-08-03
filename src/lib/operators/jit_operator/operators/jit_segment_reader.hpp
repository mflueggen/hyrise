#pragma once

#include "../jit_types.hpp"
#include "../jit_utils.hpp"

namespace opossum {

#define JIT_EXPLICIT_FUNCTION(r, function_name, type) \
  virtual std::optional<BOOST_PP_TUPLE_ELEM(3, 0, type)> function_name(JitRuntimeContext& context, const BOOST_PP_TUPLE_ELEM(3, 0, type)) { \
    Fail("Data type " + type_to_string<BOOST_PP_TUPLE_ELEM(3, 0, type)>() + " does not match reader data type."); \
  }


class BaseJitSegmentReaderWrapper;

/* Base class for all segment readers.
 * We need this class, so we can store a number of JitSegmentReaders with different template
 * specializations in a common data structure.
 */
class BaseJitSegmentReader {
public:
  virtual ~BaseJitSegmentReader() = default;
  virtual void read_and_store_value(JitRuntimeContext& context) = 0;
  BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_FUNCTION, read_and_get_value, JIT_DATA_TYPE_INFO_WITH_VALUE_ID)
  BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_FUNCTION, read_and_store_and_get_value, JIT_DATA_TYPE_INFO_WITH_VALUE_ID)
  virtual std::shared_ptr<BaseJitSegmentReaderWrapper> create_wrapper(const size_t reader_index) const = 0;
};

/* JitSegmentReaders wrap the segment iterable interface used by most operators and makes it accessible
 * to the JitOperatorWrapper.
 *
 * Why we need this wrapper:
 * Most operators access data by creating a fixed number (usually one or two) of segment iterables and
 * then immediately use those iterators in a lambda. The JitOperatorWrapper, on the other hand, processes
 * data in a tuple-at-a-time fashion and thus needs access to an arbitrary number of segment iterators
 * at the same time.
 *
 * We solve this problem by introducing a template-free super class to all segment iterators. This allows us to
 * create an iterator for each input segment (before processing each chunk) and store these iterators in a
 * common vector in the runtime context.
 * We then use JitSegmentReader instances to access these iterators. JitSegmentReaders are templated with the
 * type of iterator they are supposed to handle. They are initialized with an input_index and a tuple entry.
 * When requested to read a value, they will access the iterator from the runtime context corresponding to their
 * input_index and copy the value to their JitTupleEntry.
 *
 * All segment readers have a common template-free base class. That allows us to store the segment readers in a
 * vector as well and access all types of segments with a single interface.
 */
template <typename Iterator, typename DataType, bool Nullable>
class JitSegmentReader : public BaseJitSegmentReader {
public:
  using ReaderDataType = DataType;
  JitSegmentReader(const Iterator& iterator, const size_t tuple_index)
          : _iterator{iterator}, _tuple_index{tuple_index} {}

  // Reads a value from the _iterator into the _tuple_entry and increments the _iterator.
  std::optional<DataType> read_and_get_value(JitRuntimeContext& context, const DataType) {
    const size_t current_offset = context.chunk_offset;
    _iterator += current_offset - _chunk_offset;
    _chunk_offset = current_offset;
    const auto value = *_iterator;
    // clang-format off
    if constexpr (Nullable) {
      if (value.is_null()) {
        return std::nullopt;
      }
    }
    return value.value();
    // clang-format on
  }

  std::optional<DataType> read_and_store_and_get_value(JitRuntimeContext& context, const DataType) {
    const size_t current_offset = context.chunk_offset;
    _iterator += current_offset - _chunk_offset;
    _chunk_offset = current_offset;
    const auto value = *_iterator;
    // clang-format off
    if constexpr (Nullable) {
      context.tuple.set_is_null(_tuple_index, value.is_null());
      if (value.is_null()) {
        return std::nullopt;
      }
    }
    context.tuple.set<DataType>(_tuple_index, value.value());
    return value.value();
    // clang-format on
  }

  void read_and_store_value(JitRuntimeContext& context) {
    read_and_store_and_get_value(context, DataType{});
  }

  std::shared_ptr<BaseJitSegmentReaderWrapper> create_wrapper(const size_t reader_index) const final;

private:
  Iterator _iterator;
  const size_t _tuple_index;
  size_t _chunk_offset = 0;
};


class BaseJitSegmentReaderWrapper {
public:
  explicit BaseJitSegmentReaderWrapper(const size_t reader_index) : _reader_index(reader_index) {}
  virtual ~BaseJitSegmentReaderWrapper() = default;

  BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_FUNCTION, read_and_get_value, JIT_DATA_TYPE_INFO_WITH_VALUE_ID)
  virtual void read_and_store_value(JitRuntimeContext& context) = 0;

  virtual bool compare_type_and_update_use_cast(JitRuntimeContext& context) = 0;

  bool store_read_value = false;

protected:
  const size_t _reader_index;
};

#define JIT_EXPLICIT_READ_WRAPPER_FUNCTION(r, _, type) \
  std::optional<BOOST_PP_TUPLE_ELEM(3, 0, type)> read_and_get_value(JitRuntimeContext& context, const BOOST_PP_TUPLE_ELEM(3, 0, type)) final { \
    std::shared_ptr<BaseJitSegmentReader> reader; \
    if constexpr (std::is_same_v<BOOST_PP_TUPLE_ELEM(3, 0, type), ValueID>) { \
      reader = context.inputs[_reader_index].value_id_reader; \
    } else { \
      reader = context.inputs[_reader_index].real_value_reader; \
    } \
    if (store_read_value) { \
      if constexpr (std::is_same_v<BOOST_PP_TUPLE_ELEM(3, 0, type), ReaderDataType>) { \
        if (_use_cast) { \
          return std::static_pointer_cast<JitSegmentReader>(reader)->read_and_store_and_get_value(context, BOOST_PP_TUPLE_ELEM(3, 0, type){}); \
        } \
      } \
      return reader->read_and_store_and_get_value(context, BOOST_PP_TUPLE_ELEM(3, 0, type){}); \
    } else { \
      if constexpr (std::is_same_v<BOOST_PP_TUPLE_ELEM(3, 0, type), ReaderDataType>) { \
        if (_use_cast) { \
          return std::static_pointer_cast<JitSegmentReader>(reader)->read_and_get_value(context, BOOST_PP_TUPLE_ELEM(3, 0, type){}); \
        } \
      } \
      return reader->read_and_get_value(context, BOOST_PP_TUPLE_ELEM(3, 0, type){}); \
    } \
  }

template <typename JitSegmentReader>
class JitSegmentReaderWrapper : public BaseJitSegmentReaderWrapper {
public:
  using ReaderDataType = typename JitSegmentReader::ReaderDataType;
  static constexpr bool can_read_value_ids = std::is_same_v<ReaderDataType, ValueID>;

  explicit JitSegmentReaderWrapper(const size_t reader_index) : BaseJitSegmentReaderWrapper(reader_index) {}

  // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
  BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_READ_WRAPPER_FUNCTION, _, JIT_DATA_TYPE_INFO_WITH_VALUE_ID)

  void read_and_store_value(JitRuntimeContext& context) {
    auto& reader = _read_value_ids ? context.inputs[_reader_index].value_id_reader : context.inputs[_reader_index].real_value_reader;
    if (_use_cast) {
      std::static_pointer_cast<JitSegmentReader>(reader)->read_and_store_value(context);
    } else {
      reader->read_and_store_value(context);
    }
  }

  bool compare_type_and_update_use_cast(JitRuntimeContext& context) final {
    if constexpr (can_read_value_ids) {
      _read_value_ids = static_cast<bool>(context.inputs[_reader_index].value_id_reader);
    }
    auto& reader = _read_value_ids ? context.inputs[_reader_index].value_id_reader : context.inputs[_reader_index].real_value_reader;
    _use_cast = static_cast<bool>(std::dynamic_pointer_cast<JitSegmentReader>(reader));
    return _use_cast;
  }

private:
  bool _use_cast = true;
  bool _read_value_ids = can_read_value_ids;
};

template <typename Iterator, typename DataType, bool Nullable>
std::shared_ptr<BaseJitSegmentReaderWrapper> JitSegmentReader<Iterator, DataType, Nullable>::create_wrapper(const size_t reader_index) const {
  return std::make_shared<JitSegmentReaderWrapper<JitSegmentReader<Iterator, DataType, Nullable>>>(reader_index);
}

// cleanup
#undef JIT_EXPLICIT_FUNCTION
#undef JIT_EXPLICIT_READ_WRAPPER_FUNCTION

}  // namespace opossum
