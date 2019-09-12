from .schema import *

import numpy


def _make_mask(value, length):
    if value is None:
        return _make_mask(True, length)

    if isinstance(value, bool):
        assert length >= 0

        if length == 0:
            return b''

        n = length // 8

        m = length % 8
        if m != 0:
            n += 1

        if value:
            mask = numpy.repeat(numpy.uint8(255), n)
        else:
            mask = numpy.zeros(n, numpy.uint8)

        mask[-1] >>= m
        mask[-1] <<= m

        return mask

    with memoryview(value) as view:
        if length % 8 == 0:
            assert len(view) == length // 8
        else:
            assert len(view) == length // 8 + 1

    return value


def _inspect_offsets(values, offsets=None, counts=None):
    if counts is not None:
        counts = numpy.array(counts, numpy.int32, copy=False)
    else:
        with memoryview(offsets) as view:
            assert len(view) % 4 == 0
            length = len(view) // 4
            assert length > 0

            counts = numpy.ndarray(length, numpy.int32, view)
            counts = numpy.diff(counts, prepend=counts[0])

    assert numpy.sum(counts) == len(values)
    assert all(counts >= 0)

    return counts


class Array():
    def __init__(self, data, mask=None):
        self._data = data

        with memoryview(self._data) as view:
            assert len(view) % self.schema.byte_width == 0
            self._length = len(view) // self.schema.byte_width

        self._mask = _make_mask(mask, self._length)

    def __len__(self):
        return self._length

    @property
    def data(self):
        return self._data

    @property
    def mask(self):
        return self._mask

    def numpy_data(self):
        return numpy.ndarray(len(self), self.schema.name, self.data)

    def numpy_mask(self):
        values = numpy.unpackbits(self.mask).astype(bool)

        assert length <= len(values) < length + 8
        assert not any(values[length:])

        return values[:length]


class NullArray(Array):
    schema = Null()

    def __init__(self, length):
        self._length = length
        self._mask = _make_mask(False, length)

    def numpy_data(self):
        return numpy.repeat(None, self._length)


class BoolArray(Array):
    schema = Bool()

    def accept(self, visitor):
        return visitor.visit_null(self)


class Int8Array(Array):
    schema = Int8()

    def accept(self, visitor):
        return visitor.visit_int8(self)


class Int16Array(Array):
    schema = Int16()

    def accept(self, visitor):
        return visitor.visit_int16(self)


class Int32Array(Array):
    schema = Int32()

    def accept(self, visitor):
        return visitor.visit_int32(self)


class Int64Array(Array):
    schema = Int64()

    def accept(self, visitor):
        return visitor.visit_int64(self)


class UInt8Array(Array):
    schema = UInt8()

    def accept(self, visitor):
        return visitor.visit_uint8(self)


class UInt16Array(Array):
    schema = UInt16()

    def accept(self, visitor):
        return visitor.visit_uint16(self)


class UInt32Array(Array):
    schema = UInt32()

    def accept(self, visitor):
        return visitor.visit_uint32(self)


class UInt64Array(Array):
    schema = UInt64()

    def accept(self, visitor):
        return visitor.visit_uint64(self)


class Float16Array(Array):
    schema = Float16()

    def accept(self, visitor):
        return visitor.visit_float16(self)


class Float32Array(Array):
    schema = Float32()

    def accept(self, visitor):
        return visitor.visit_float32(self)


class Float64Array(Array):
    schema = Float64()

    def accept(self, visitor):
        return visitor.visit_float64(self)


class DateArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Date)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_date(self)

    @property
    def schema(self):
        return self._schema


class TimestampArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Timestamp)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_timestamp(self)

    @property
    def schema(self):
        return self._schema


class TimeArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Time)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_time(self)

    @property
    def schema(self):
        return self._schema


class OpaqueArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Opaque)
        self._schema = schema

    def accept(self, visitor):
        return visitor.visit_opaque(self)

    @property
    def schema(self):
        return self._schema


class BinaryArray(Array):
    def __init__(self, values, mask=None, *, offsets=None, counts=None):
        self._counts = _inspect_offsets(values, offsets, counts)
        self._length = len(self._counts) - 1
        self._values = values
        self._mask = _make_mask(mask, self._length)

    @property
    def values(self):
        return self._values

    @property
    def counts(self):
        return self._counts

    @property
    def data(self):
        raise NotImplementedError()

    def numpy_data(self):
        raise NotImplementedError()


class BytesArray(BinaryArray):
    schema = Bytes()


class Utf8Array(BinaryArray):
    schema = Utf8()


class DictionaryArray(Array):
    def __init__(self, index: Array, value: Array):
        self._index = index
        self._value = value

    def __len__(self):
        return len(self._index)

    @property
    def data(self):
        return self._index.data

    @property
    def mask(self):
        return self._index.mask

    def numpy_data(self):
        raise self._index.numpy_data()

    def numpy_mask(self):
        raise self._index.numpy_mask()


class OrderedArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Ordered(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_list(self)


class FactorArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Factor(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_factor(self)


class ListArray(Array):
    def __init__(self, values: Array, mask=None, *, offsets=None, counts=None):
        self._schema = List(values.schema)
        self._counts = _inspect_offsets(values, offsets, counts)
        self._length = len(self._counts) - 1
        self._values = values
        self._mask = _make_mask(mask, self._length)

    @property
    def schema(self):
        return self._schema

    def accept(self, visitor):
        return visitor.visit_list(self)

    @property
    def values(self):
        return self._values

    @property
    def counts(self):
        return self._counts

    @property
    def data(self):
        raise NotImplementedError()

    def numpy_data(self):
        raise NotImplementedError()


class StructArray(Array):
    def __init__(self, fields, mask=None):
        self.schema = Struct([(k, v.schema) for k, v in fields])
        self._fields = fields
        self._mask = _make_mask(mask, self._length)

        self._length = None
        for _, v in fields:
            if self._length is None:
                self._length = len(v)
            else:
                assert self._length == len(v)

    @property
    def schema(self):
        return self._schema

    def accept(self, visitor):
        return visitor.visit_struct(self)

    @property
    def fields(self):
        raise self._fields

    @property
    def data(self):
        raise NotImplementedError()

    def numpy_data(self):
        raise NotImplementedError()
