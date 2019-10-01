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


class _SchemaToArrow(Visitor):
    def visit_bool(self, schema):
        return pyarrow.bool_()

    def visit_numeric(self, schema):
        return getattr(pyarrow, schema.name)()

    def visit_date(self, schema):
        return getattr(pyarrow, f'date{schema.byte_width * 8}')()

    def visit_timestamp(self, schema):
        return pyarrow.timestamp(schema.unit)

    def visit_time(self, schema):
        return getattr(pyarrow, f'time{schema.byte_width * 8}')(schema.unit)

    def visit_opaque(self, schema):
        return pyarrow.binary(schema.byte_width)

    def visit_bytes(self, schema):
        return pyarrow.binary()

    def visit_utf8(self, schema):
        return pyarrow.utf8()

    def visit_dictionary(self, schema):
        index = schema.index.accept(self)
        value = schema.value.accept(self)
        ordered = isinstance(schema, Ordered)
        return pyarrow.dictionary(index, value, ordered)

    def visit_list(self, schema):
        value = schema.value.accept(self)
        return pyarrow.list_(value)

    def visit_struct(self, schema):
        fields = [pyarrow.field(k, v.accept(self)) for k, v in schema.fields]
        return pyarrow.struct(fields)


def _schema_to_arrow(self):
    return self.accept(_SchemaToArrow())


def _schema_from_arrow(dtype):
    assert isinstance(dtype, pyarrow.DataType)

    if pyarrow.types.is_boolean(dtype):
        return Bool()

    if pyarrow.types.is_int8(dtype):
        return Int8()

    if pyarrow.types.is_int16(dtype):
        return Int16()

    if pyarrow.types.is_int32(dtype):
        return Int32()

    if pyarrow.types.is_int64(dtype):
        return Int64()

    if pyarrow.types.is_uint8(dtype):
        return UInt8()

    if pyarrow.types.is_uint16(dtype):
        return UInt16()

    if pyarrow.types.is_uint32(dtype):
        return UInt32()

    if pyarrow.types.is_uint64(dtype):
        return UInt64()

    if pyarrow.types.is_float16(dtype):
        return Float16()

    if pyarrow.types.is_float32(dtype):
        return Float32()

    if pyarrow.types.is_float64(dtype):
        return Float64()

    if pyarrow.types.is_date32(dtype):
        return Date('d')

    if pyarrow.types.is_date64(dtype):
        return Date('ms')

    if pyarrow.types.is_timestamp(dtype):
        assert dtype.tz is None
        return Timestamp(dtype.unit)

    if dtype.equals(pyarrow.time32('s')):
        return Time('s')

    if dtype.equals(pyarrow.time32('ms')):
        return Time('ms')

    if dtype.equals(pyarrow.time64('us')):
        return Time('us')

    if dtype.equals(pyarrow.time64('ns')):
        return Time('ns')

    if pyarrow.types.is_binary(dtype):
        return Bytes()

    if pyarrow.types.is_string(dtype):
        return Utf8()

    if pyarrow.types.is_fixed_size_binary(dtype):
        return Opaque(dtype.byte_width)

    if pyarrow.types.is_dictionary(dtype):
        stype = Ordered if dtype.ordered else Factor
        index = _schema_from_arrow(dtype.index_type)
        value = _schema_from_arrow(dtype.value_type)
        return stype(index, value)

    if pyarrow.types.is_list(dtype):
        value = _schema_from_arrow(dtype.value_type)
        return List(value)

    if pyarrow.types.is_struct(dtype):
        fields = ((v.name, _schema_from_arrow(v.type)) for v in dtype)
        return Struct(fields)

    raise ValueError(f'{dtype} is not supported BSON DataFrame type')


class _ArrayToArrow(Visitor):
    def _make_buffer(self, data):
        if data is None:
            return data

        if isinstance(data, pyarrow.Buffer):
            return data

        if isinstance(data, bytes):
            return pyarrow.py_buffer(data)

        if isinstance(data, numpy.ndarray):
            return pyarrow.py_buffer(data.tobytes())

        with memoryview(data) as view:
            return pyarrow.py_buffer(view.tobytes())

    def _make_mask(self, array):
        mask = numpy.frombuffer(array.mask, numpy.uint8)
        mask = numpy.unpackbits(mask)
        null = len(array) - numpy.sum(mask)
        mask = None if null == 0 else numpy.packbits(mask, bitorder='little')
        return null, mask

    def _make_offsets(self, array):
        counts = numpy.frombuffer(array.counts, numpy.int32)
        return numpy.cumsum(counts, dtype=numpy.int32)

    def _make_array(self, array, data=None, *, offsets=None, children=None):
        dtype = _schema_to_arrow(array.schema)
        null_count, mask = self._make_mask(array)

        buffers = [self._make_buffer(mask)]

        if offsets is not None:
            buffers.append(self._make_buffer(offsets))

        if data is not None:
            buffers.append(self._make_buffer(data))

        return pyarrow.Array.from_buffers(dtype,
                                          len(array),
                                          buffers,
                                          null_count=null_count,
                                          offset=0,
                                          children=children)

    def visit_bool(self, array):
        data = numpy.frombuffer(array.data, bool)
        data = numpy.packbits(data, bitorder='little')
        return self._make_array(array, data)

    def visit_numeric(self, array):
        return self._make_array(array, array.data)

    def visit_date(self, array):
        return self._make_array(array, array.data)

    def visit_timestamp(self, array):
        return self._make_array(array, array.data)

    def visit_time(self, array):
        return self._make_array(array, array.data)

    def visit_opaque(self, array):
        return self._make_array(array, array.data)

    def visit_binary(self, array):
        offsets = self._make_offsets(array)
        return self._make_array(array, array.data, offsets=offsets)

    def visit_dictionary(self, array):
        index = array.index.accept(self)
        value = array.value.accept(self)
        ordered = isinstance(array.schema, Ordered)
        return pyarrow.DictionaryArray.from_arrays(index,
                                                   value,
                                                   ordered=ordered)

    def visit_list(self, array):
        offsets = self._make_offsets(array)
        children = [array.value.accept(self)]
        return self._make_array(array, offsets=offsets, children=children)

    def visit_struct(self, array):
        children = [v.accept(self) for _, v in array.fields]
        return self._make_array(array, children=children)


def _array_to_arrow(self):
    return self.accept(_ArrayToArrow())


class _ArrayFromArrow(Visitor):
    def __init__(self, array):
        assert isinstance(array, pyarrow.Array)

        self.array = array

    def _make_mask(self):
        if self.array.null_count == 0:
            return None

        mask = self.array.buffers()[0]
        mask = numpy.frombuffer(mask, numpy.uint8)
        mask = numpy.unpackbits(mask, bitorder='little')
        mask = mask[self.array.offset:(self.array.offset + len(self.array))]

        return numpy.packbits(mask)

    def _make_counts(self):
        counts = self.array.buffers()[1]
        counts = counts.slice(self.array.offset * 4, len(self.array) * 4 + 4)
        counts = numpy.frombuffer(counts, numpy.int32)
        start = counts[0]
        length = counts[-1] - start
        counts = numpy.diff(counts, prepend=start)

        return counts, start, length

    def visit_bool(self, schema):
        atype = array_type(schema)
        data = self.array.buffers()[-1]
        data = numpy.frombuffer(data, numpy.uint8)
        data = numpy.unpackbits(data, bitorder='little')
        data = data[self.array.offset:(self.array.offset + len(self.array))]
        mask = self._make_mask()
        return atype(data, mask)

    def _make_data(self, schema, start=None, length=None):
        if start is None:
            assert schema.byte_width * 8 == self.array.type.bit_width
            start = self.array.offset * schema.byte_width

        if length is None:
            assert schema.byte_width * 8 == self.array.type.bit_width
            length = len(self.array) * schema.byte_width

        data = self.array.buffers()[-1].slice(start, length)
        mask = self._make_mask()

        return data, mask

    def visit_numeric(self, schema):
        atype = array_type(schema)
        return atype(*self._make_data(schema))

    def visit_date(self, schema):
        atype = array_type(schema)
        return atype(*self._make_data(schema), schema=schema)

    def visit_timestamp(self, schema):
        atype = array_type(schema)
        return atype(*self._make_data(schema), schema=schema)

    def visit_time(self, schema):
        atype = array_type(schema)
        return atype(*self._make_data(schema), schema=schema)

    def visit_opaque(self, schema):
        atype = array_type(schema)
        return atype(*self._make_data(schema), schema=schema)

    def visit_binary(self, schema):
        atype = array_type(schema)
        counts, start, length = self._make_counts()
        data, mask = self._make_data(schema, start, length)
        return atype(data, mask, length=len(self.array), counts=counts)

    def visit_dictionary(self, schema):
        index = schema.index.accept(_ArrayFromArrow(self.array.indices))
        value = schema.value.accept(_ArrayFromArrow(self.array.dictionary))
        return array_type(schema)(index, value)

    def visit_list(self, schema):
        counts, start, length = self._make_counts()
        data = self.array.flatten().slice(start, length)
        data = schema.value.accept(_ArrayFromArrow(data))
        mask = self._make_mask()
        return ListArray(data, mask, length=len(self.array), counts=counts)

    def visit_struct(self, schema):
        fields = list()
        for i, kv in enumerate(schema.fields):
            k, v = kv
            data = self.array.field(i)
            data = v.accept(_ArrayFromArrow(data))
            fields.append((k, data))
        return StructArray(fields)


def _array_from_arrow(array):
    schema = _schema_from_arrow(array.type)
    return schema.accept(_ArrayFromArrow(array))


Schema.to_arrow = _schema_to_arrow
Schema.from_arrow = staticmethod(_schema_from_arrow)

Array.to_arrow = _array_to_arrow
Array.from_arrow = staticmethod(_array_from_arrow)
