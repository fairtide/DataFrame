# ============================================================================
# Copyright 2019 Fairtide Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

from .schema import *
from .array import *
from .visitor import *

import pyarrow


def _make_mask(array: pyarrow.Array):
    if array.null_count == 0:
        return None

    mask = array.buffers()[0]
    vals = numpy.ndarray(len(mask), numpy.uint8, mask)
    bits = numpy.unpackbits(vals, bitorder='little')
    data = bits[array.offset:(array.offset + len(array))]

    return numpy.packbits(data)


def _make_counts(buf: pyarrow.Buffer, array: pyarrow.Array):
    counts = buf.slice(array.offset * 4, len(array) * 4 + 4)
    counts = numpy.frombuffer(counts, numpy.int32)
    counts = numpy.diff(counts, prepend=counts[0])

    return counts


class _ToArrow(Visitor):
    def _make_mask(self, array):
        mask = array.numpy_mask(submask=False)

        if mask is numpy.ma.nomask:
            return None

        mask = numpy.packbits(mask, bitorder='little')
        return pyarrow.py_buffer(mask.tobytes())

    def _make_offsets(self, array):
        counts = numpy.frombuffer(array.counts, numpy.int32)
        offsets = numpy.cumsum(counts, dtype=numpy.int32)
        return pyarrow.py_buffer(offsets.tobytes())

    def visit_null(self, array):
        dtype = pyarrow.null()
        buffers = [None]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_bool(self, array):
        dtype = pyarrow.bool_()
        data = numpy.frombuffer(array.data, bool)
        bits = numpy.packbits(data, bitorder='little')
        buffers = [self._make_mask(array), pyarrow.py_buffer(bits)]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_numeric(self, array):
        dtype = getattr(pyarrow, array.schema.name)()
        buffers = [self._make_mask(array), pyarrow.py_buffer(array.data)]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_date(self, array):

        if array.schema.unit == 'd':
            dtype = pyarrow.date32()

        if array.schema.unit == 'ms':
            dtype = pyarrow.date64()

        buffers = [self._make_mask(array), pyarrow.py_buffer(array.data)]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_timestamp(self, array):
        dtype = pyarrow.timestamp(array.schema.unit)
        buffers = [self._make_mask(array), pyarrow.py_buffer(array.data)]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_time(self, array):
        dtype = getattr(pyarrow, f'time{array.schema.byte_width * 8}')
        dtype = dtype(array.schema.unit)
        buffers = [self._make_mask(array), pyarrow.py_buffer(array.data)]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_opaque(self, array):
        dtype = pyarrow.binary(array.schema.byte_width)
        buffers = [self._make_mask(array), pyarrow.py_buffer(array.data)]
        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_binary(self, array):
        if isinstance(array.schema, Bytes):
            dtype = pyarrow.binary()

        if isinstance(array.schema, Utf8):
            dtype = pyarrow.utf8()

        buffers = [
            self._make_mask(array),
            self._make_offsets(array),
            pyarrow.py_buffer(array.data)
        ]

        return pyarrow.Array.from_buffers(dtype, len(array), buffers)

    def visit_dictionary(self, array):
        index = array.index.accept(self)
        value = array.value.accept(self)
        ordered = isinstance(array.schema, Ordered)
        return pyarrow.DictionaryArray.from_arrays(index,
                                                   value,
                                                   ordered=ordered)

    def visit_list(self, array):
        values = array.value.accept(self)
        dtype = pyarrow.list_(values.type)
        buffers = [self._make_mask(array), self._make_offsets(array)]
        children = [values]
        return pyarrow.Array.from_buffers(dtype,
                                          len(array),
                                          buffers,
                                          children=children)

    def visit_struct(self, array):
        fields = [(k, v.accept(self)) for k, v in array.fields]
        dtype = pyarrow.struct([pyarrow.field(k, v.type) for k, v in fields])
        buffers = [self._make_mask(array)]
        children = [v for _, v in fields]
        return pyarrow.Array.from_buffers(dtype,
                                          len(array),
                                          buffers,
                                          children=children)


def _FromArrow(Visitor):
    def __init__(self, array):
        self.array = array

    def _make_data(self):
        pass

    def _make_mask(self):
        pass

    def visit_null(self, schema):
        return NullArray(len(self.array))

    def visit_bool(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask())

    def visit_numeric(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask())

    def visit_date(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask(),
                                  schema=schema)

    def visit_timestamp(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask(),
                                  schema=schema)

    def visit_time(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask(),
                                  schema=schema)

    def visit_opaque(self, schema):
        return array_type(schema)(self._make_data(schema.byte_width),
                                  self._make_mask(),
                                  schema=schema)

    def visit_binary(self, schema):
        counts, start, length = self._make_counts()
        return array_type(schema)(self._make_data(start, length),
                                  self._make_mask(),
                                  counts=counts)

    def vist_dictionary(self, schema):
        index = schema.index.accept(self.array.indices)
        value = schema.value.accept(self.array.dictionary)
        return array_type(schema)(index, value)

    def visit_list(self, schema):
        counts, start, end = self._make_counts()
        values = self.array.flatten().slice(start, length)
        return ListArray(schema.value.accept(values), counts=counts)

    def visit_struct(self, schema):
        fields = list()
        for i, kv in enumerate(schema.fields):
            k, v = kv
            fields.append(v.accept(self.array.field[i]))
        return StructArray(fields)


def arrow_schema(data):
    if pyarrow.types.is_null(data.type):
        return Null()

    if pyarrow.types.is_boolean(data.type):
        return Bool()

    if pyarrow.types.is_int8(data.type):
        return Int8()

    if pyarrow.types.is_int16(data.type):
        return Int16()

    if pyarrow.types.is_int32(data.type):
        return Int32()

    if pyarrow.types.is_int64(data.type):
        return Int64()

    if pyarrow.types.is_uint8(data.type):
        return UInt8()

    if pyarrow.types.is_uint16(data.type):
        return UInt16()

    if pyarrow.types.is_uint32(data.type):
        return UInt32()

    if pyarrow.types.is_uint64(data.type):
        return UInt64()

    if pyarrow.types.is_float16(data.type):
        return Float16()

    if pyarrow.types.is_float32(data.type):
        return Float32()

    if pyarrow.types.is_float64(data.type):
        return Float64()

    if pyarrow.types.is_date32(data.type):
        return Date('d')

    if pyarrow.types.is_date64(data.type):
        return Date('ms')

    if pyarrow.types.is_timestamp(data.type):
        assert data.type.tz is None
        return Timestamp(data.type.unit)

    if data.type.equals(pyarrow.time32('s')):
        return Time('s')

    if data.type.equals(pyarrow.time32('ms')):
        return Time('ms')

    if data.type.equals(pyarrow.time64('us')):
        return Time('us')

    if data.type.equals(pyarrow.time64('ns')):
        return Time('ns')

    if pyarrow.types.is_binary(data.type):
        return Bytes()

    if pyarrow.types.is_string(data.type):
        return Utf8()

    if pyarrow.types.is_fixed_size_binary(data.type):
        return Opaque(data.type.byte_width)

    if pyarrow.types.is_dictionary(data.type):
        index = arrow_schema(data.indices)
        value = arrow_schema(data.dictionary)
        if data.type.ordered:
            return Ordered(index, value)
        else:
            return Factor(index, value)

    if pyarrow.types.is_list(data.type):
        return List(arrow_schema(data.flatten()))

    if pyarrow.types.is_struct(data.type):
        return Struct([(v.name, arrow_schema(data.field(i)))
                       for i, v in enumerate(data.type)])


def to_arrow(array):
    return array.accept(_ToArrow())


def from_arrow(data):
    schema = arrow_schema(data)

    if schema is None:
        raise ValueError(f'Unable to deduce schema from dtype {data.dtype}')

    return schema.accept(_FromArrow(data))
