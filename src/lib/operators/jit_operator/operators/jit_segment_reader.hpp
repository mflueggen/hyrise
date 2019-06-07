#pragma once

#include "../jit_types.hpp"

namespace opossum {

class BaseJitSegmentReaderWrapper;

/* Base class for all segment readers.
 * We need this class, so we can store a number of JitSegmentReaders with different template
 * specializations in a common data structure.
 */
class BaseJitSegmentReader {
public:
  virtual ~BaseJitSegmentReader() = default;
  virtual void read_value(JitRuntimeContext& context) = 0;
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
  void read_value(JitRuntimeContext& context) {
    const auto& value = *_iterator;
    ++_iterator;
    // clang-format off
    if constexpr (Nullable) {
      context.tuple.set_is_null(_tuple_index, value.is_null());
      if (!value.is_null()) {
        context.tuple.set<DataType>(_tuple_index, value.value());
      }
    } else {
      context.tuple.set<DataType>(_tuple_index, value.value());
    }
    // clang-format on
  }

  virtual std::shared_ptr<BaseJitSegmentReaderWrapper> create_wrapper(const size_t reader_index) const;

private:
  Iterator _iterator;
  const size_t _tuple_index;
};


class BaseJitSegmentReaderWrapper {
public:
  BaseJitSegmentReaderWrapper(const size_t reader_index, const bool read_value_ids) : read_value_ids(read_value_ids), _reader_index(reader_index) {}
  virtual ~BaseJitSegmentReaderWrapper() = default;

  virtual void read_value(JitRuntimeContext& context) = 0;

  virtual bool compare_type_and_update_use_cast(JitRuntimeContext& context) { return true; }

  bool read_value_ids;
  bool store_read_value = false;

protected:
  const size_t _reader_index;
};

template <typename JitSegmentReader>
class JitSegmentReaderWrapper : public BaseJitSegmentReaderWrapper {
public:
  //using BaseJitSegmentReaderWrapper::BaseJitSegmentReaderWrapper;
  static constexpr bool can_read_value_ids = std::is_same_v<typename JitSegmentReader::ReaderDataType, ValueID>;

  JitSegmentReaderWrapper(const size_t reader_index) : BaseJitSegmentReaderWrapper(reader_index, can_read_value_ids) {}

  // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
  void read_value(JitRuntimeContext& context) final {
    auto& reader = read_value_ids ? context.inputs[_reader_index].value_id_reader : context.inputs[_reader_index].real_value_reader;
    if (_use_cast) {
      std::static_pointer_cast<JitSegmentReader>(reader)->read_value(context);
    } else {
      reader->read_value(context);
    }
  }

  bool compare_type_and_update_use_cast(JitRuntimeContext& context) final {
    if constexpr (can_read_value_ids) {
      read_value_ids = static_cast<bool>(context.inputs[_reader_index].value_id_reader);
    }
    auto& reader = read_value_ids ? context.inputs[_reader_index].value_id_reader : context.inputs[_reader_index].real_value_reader;
    _use_cast = static_cast<bool>(std::dynamic_pointer_cast<JitSegmentReader>(reader));
    return _use_cast;
  }

private:
  bool _use_cast = true;
};

template <typename Iterator, typename DataType, bool Nullable>
std::shared_ptr<BaseJitSegmentReaderWrapper> JitSegmentReader<Iterator, DataType, Nullable>::create_wrapper(const size_t reader_index) const {
  return std::make_shared<JitSegmentReaderWrapper<JitSegmentReader<Iterator, DataType, Nullable>>>(reader_index);
}

}  // namespace opossum
