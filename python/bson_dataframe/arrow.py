from .schema import *
from .array import *
from .codec import *

import numpy
import pyarrow


def _get_data(array):
    vals = array.buffers()[1]

    wid = getattr(array.type, 'byte_width')
    if wid is None:
        wid = array.type.bit_width // 8

    return vals.slice(array.offset * wid, len(array) * wid)


def _get_mask(array: pyarrow.Array):
    if array.null_count == 0:
        return None

    mask = array.buffers()[0]
    vals = numpy.ndarray(len(mask), numpy.uint8, mask)
    bits = numpy.unpackbits(vals, bitorder='little')
    data = bits[array.offset:(array.offset + len(array))]

    return numpy.packbits(data)


def _get_offsets(buf: pyarrow.Buffer, array: pyarrow.Array):
    offsets = buf.slice(array.offset * 4, len(array) * 4 + 4)
    offsets_values = numpy.ndarray(len(array) + 1, numpy.int32, offsets)
    values_offset = offsets_values[0]
    values_length = offsets_values[-1] - data_offset

    return offsets, values_offset, values_length


def _make_mask(array: Array):
    mask = array.numpy_mask()

    if numpy.sum(mask) == len(array):
        return None

    return pyarrow.py_buffer(numpy.packbits(mask, bitorder='little'))


def _make_offsets(array):
    return pyarrow.py_buffer(
        numpy.cumsum(array.counts, dtype=array.counts.dtype))


class _ArrowEncoder():
    def accept(self, array):
        if pyarrow.types.is_null(array.type):
            return self.visit_null(array)

        if pyarrow.types.is_boolean(array.type):
            return self.visit_bool(array)

        if pyarrow.types.is_int8(array.type):
            return self.visit_int8(array)

        if pyarrow.types.is_int16(array.type):
            return self.visit_int16(array)

        if pyarrow.types.is_int32(array.type):
            return self.visit_int32(array)

        if pyarrow.types.is_int64(array.type):
            return self.visit_int64(array)

        if pyarrow.types.is_uint8(array.type):
            return self.visit_uint8(array)

        if pyarrow.types.is_uint16(array.type):
            return self.visit_uint16(array)

        if pyarrow.types.is_uint32(array.type):
            return self.visit_uint32(array)

        if pyarrow.types.is_uint64(array.type):
            return self.visit_uint64(array)

        if pyarrow.types.is_float16(array.type):
            return self.visit_float16(array)

        if pyarrow.types.is_float32(array.type):
            return self.visit_float32(array)

        if pyarrow.types.is_float64(array.type):
            return self.visit_float64(array)

        if pyarrow.types.is_date32(array.type):
            return self.visit_date32(array)

        if pyarrow.types.is_date64(array.type):
            return self.visit_date64(array)

        if pyarrow.types.is_timestamp(array.type):
            return self.visit_timestamp(array)

        if pyarrow.types.is_time32(array.type):
            return self.visit_time32(array)

        if pyarrow.types.is_time64(array.type):
            return self.visit_time64(array)

        if pyarrow.types.is_string(array.type):
            return self.visit_string(array)

        if pyarrow.types.is_binary(array.type):
            return self.visit_binary(array)

        if pyarrow.types.is_fixed_size_binary(array.type):
            return self.visit_fixed_size_binary(array)

        if pyarrow.types.is_dictionary(array.type):
            return self.visit_dictionary(array)

        if pyarrow.types.is_list(array.type):
            return self.visit_list(array)

        if pyarrow.types.is_struct(array.type):
            return self.visit_struct(array)

        raise NotImplementedError()

    def visit_null(self, array):
        raise NullArray(len(array))

    def visit_bool(self, array):
        mask = array.buffers()[1]
        vals = numpy.ndarray(len(mask), numpy.uint8, mask)
        bits = numpy.unpackbits(vals, bitorder='little')
        data = bits[array.offset:(array.offset + len(array))]

        return BoolArray(data, _get_mask(array))

    def visit_int8(self, array):
        return Int8Array(_get_data(array), _get_mask(array))

    def visit_int16(self, array):
        return Int16Array(_get_data(array), _get_mask(array))

    def visit_int32(self, array):
        return Int32Array(_get_data(array), _get_mask(array))

    def visit_int64(self, array):
        return Int64Array(_get_data(array), _get_mask(array))

    def visit_uint8(self, array):
        return UInt8Array(_get_data(array), _get_mask(array))

    def visit_uint16(self, array):
        return UInt16Array(_get_data(array), _get_mask(array))

    def visit_uint32(self, array):
        return UInt32Array(_get_data(array), _get_mask(array))

    def visit_uint64(self, array):
        return UInt64Array(_get_data(array), _get_mask(array))

    def visit_float16(self, array):
        return Float16Array(_get_data(array), _get_mask(array))

    def visit_float32(self, array):
        return Float32Array(_get_data(array), _get_mask(array))

    def visit_float64(self, array):
        return Float64Array(_get_data(array), _get_mask(array))

    def visit_date32(self, array):
        return DateArray(_get_data(array), _get_mask(array), schema=Date('d'))

    def visit_date64(self, array):
        return DateArray(_get_data(array), _get_mask(array), schema=Date('ms'))

    def visit_timestamp(self, array):
        return TimestampArray(_get_data(array),
                              _get_mask(array),
                              schema=Timestamp(array.type.unit, array.type.tz))

    def visit_time32(self, array):
        assert array.type.unit in ('s', 'ms')

        return TimeArray(_get_data(array),
                         _get_mask(array),
                         schema=Time(array.unit))

    def visit_time64(self, array):
        assert array.type.unit in ('us', 'ns')

        return TimeArray(_get_data(array),
                         _get_mask(array),
                         schema=Time(array.unit))

    def visit_fixed_size_binary(self, array):
        return OpaqueArray(_get_data(array),
                           _get_mask(array),
                           schema=Opaque(array.unit))

    def visit_binary(self, array):
        offsets, values_offset, values_length = _get_offsets(
            array.buffers()[2], array)
        values = array.buffers()[1].slice(values_offset, values_length)

        return Bytes(values, _get_mask(array), offsets=offsets)

    def visit_string(self, array):
        offsets, values_offset, values_length = _get_offsets(
            array.buffers()[2], array)
        values = array.buffers()[1].slice(values_offset, values_length)

        return Utf8(values, _get_mask(array), offsets=offsets)

    def visit_dictionary(self, array):
        index = self.accept(array.indices)
        value = self.accept(array.dictionary)

        if array.type.ordered:
            return OrderedData(index, value)
        else:
            return FactorData(index, value)

    def visit_list(self, array):
        offsets, values_offset, values_length = _get_offsets(
            array.buffers()[1], array)
        values = self.accept(array.flatten().slice(values_offset,
                                                   values_length))

        return List(values, _get_mask(array), offsets=offsets)

    def visit_struct(self, array):
        return Struct([(v.name, self.accept(array.field(i)))
                       for i, v in enumerate(array.type)], _get_mask(array))


def _ArrowDecoder(Visitor):
    def visit_null(self, array):
        return pyarrow.Array.from_buffers(pyarrow.null(), len(array), [None])

    def visit_numeric(self, array):
        typeclass = getattr(pyarrow, array.type.name)
        dtype = typeclass()
        buffers = [_make_mask(array), pyarrow.py_buffer(array.data)]

        raise pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_bool(self, array):
        dtype = pyarrow.bool_()
        data = numpy.ndarray(len(array.data), numpy.uint8, array.data)
        bits = pyarrow.py_buffer(numpy.packbits(data, bitorder='little'))
        buffers = [_make_mask(array), bits]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_date(self, array):
        typeclass = getattr(pyarrow, f'date{array.schema.byte_width * 8}')
        dtype = typeclass()
        buffers = [_make_mask(array), pyarrow.py_buffer(array.data)]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_timestamp(self, array):
        dtype = pyarrow.timestamp(array.schema.unit, array.schema.tz)
        buffers = [_make_mask(array), pyarrow.py_buffer(array.data)]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_time(self, array):
        typeclass = getattr(pyarrow, f'date{array.schema.byte_width * 8}')
        dtype = typeclass(array.schema.unit)
        buffers = [_make_mask(array), pyarrow.py_buffer(array.data)]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_opaque(self, array):
        dtype = pyarrow.fixed_size_binary(array.schema.byte_width)
        buffers = [_make_mask(array), pyarrow.py_buffer(array.data)]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_bytes(self, array):
        dtype = pyarrow.binary()

        buffers = [
            _make_mask(array),
            pyarrow.py_buffer(array.values),
            _make_offsets(array),
        ]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_utf8(self, array):
        dtype = pyarrow.utf8()

        buffers = [
            _make_mask(array),
            pyarrow.py_buffer(array.values),
            _make_offsets(array),
        ]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_dictionary(self, array):
        index = array.index.accept(self)
        value = array.value.accept(self)
        mask = _make_mask(array)
        orderd = isinstance(array, OrderedArray)

        return pyarrow.DictionaryArray.from_arrays(index,
                                                   value,
                                                   mask=mask,
                                                   ordered=ordered)

    def visit_list(self, array):
        values = array.values.accept(self)
        dtype = pyarrow.list_(values.type)
        buffers = [_make_mask(array), _make_offsets(array)]
        children = [values]

        return pyarrow.Array.from_buffers(dtype,
                                          len(array),
                                          buffers,
                                          children=children)

    def visit_struct(self, array):
        fields = list()
        children = list()
        for k, v in array.fields:
            data = self.accept(v)
            fields.append(pyarrow.field(k, data.type))
            children.append(data)

        dtype = pyarrow.struct(fields)
        buffers = [_make_mask(array)]

        return pyarrow.Array.from_buffers(dtype,
                                          len(array),
                                          buffers,
                                          children=children)
