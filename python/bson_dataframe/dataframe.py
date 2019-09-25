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

import numpy
import pandas


def _make_counts(data):
    counts = numpy.ndarray(len(data) + 1, numpy.int32)
    counts[0] = 0
    for i, v in enumerate(data, 1):
        counts[i] = len(v)

    return counts


def _all_valid(length, mask):
    if len(mask) == 0:
        return True

    if any(mask[:-1] != numpy.uint8(255)):
        return False

    bits = numpy.unpackbits(mask[-1:])
    nbits = int(numpy.sum(bits) + len(mask) * 8 - 8)

    return nbits == length


def _make_ndarray(data, array):
    mask = numpy.frombuffer(array.mask, numpy.uint8)
    if _all_valid(len(array), mask):
        return data
    else:
        mask = numpy.unpackbits(mask).astype(bool, copy=False)
        mask = mask[:len(array)]
        return numpy.ma.masked_array(data, ~mask)


class _ToNumpy(Visitor):
    def visit_null(self, array):
        return numpy.ndarray(len(array), dtype=object)

    def visit_numeric(self, array):
        data = numpy.frombuffer(array.data, array.schema.name)
        return _make_ndarray(data, array)

    def visit_date(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')

        if array.schema.unit == 'd':
            data = data.astype('datetime64[D]', copy=False)

        if array.schema.unit == 'ms':
            data = data.astype('datetime64[ms]', copy=False)

        return _make_ndarray(data, array)

    def visit_timestamp(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')
        data = data.astype(f'datetime64[{array.schema.unit}]', copy=False)
        return _make_ndarray(data, array)

    def visit_time(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')
        data = data.astype(f'timedelta64[{array.schema.unit}]', copy=False)
        return _make_ndarray(data, array)

    def visit_opaque(self, array):
        data = numpy.frombuffer(array.data, f'S{array.schema.byte_width}')
        return _make_ndarray(data, array)

    def visit_bytes(self, array):
        values = array.value
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]]

        return _make_ndarray(data, array)

    def visit_utf8(self, array):
        values = array.value
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]].decode('utf8')

        return _make_ndarray(data, array)

    def visit_list(self, array):
        values = array.value.accept(self)
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]]

        return _make_ndarray(data, array)

    def visit_struct(self, array):
        fields = [(k, v.accept(self)) for k, v in array.fields]
        dtype = [(k, v.dtype) for k, v in fields]

        data = numpy.ndarray(len(array), dtype)
        for k, v in fields:
            data[k] = v

        return _make_ndarray(data, array)


class _FromNumpy(Visitor):
    def __init__(self, data, mask=None):
        self.data = data
        self.mask = mask

        if isinstance(data, numpy.ma.masked_array):
            self.data = data.data
            if self.mask is None:
                self.mask = ~data.mask

        assert isinstance(self.data, numpy.ndarray)

        if isinstance(self.mask, numpy.ndarray):
            self.mask = numpy.packbits(self.mask)

    def visit_null(self, schema):
        for v in self.data:
            assert v is None
        return NullArray(len(self.data))

    def visit_numeric(self, schema):
        data = self.data.astype(schema.name, copy=False)
        return array_type(schema)(data, self.mask)

    def visit_date(self, schema):
        if schema.unit == 'd':
            data = self.data.astype('datetime64[D]', copy=False)
        if schema.unit == 'ms':
            data = self.data.astype('datetime64[ms]', copy=False)
        data = data.astype(f'i{schema.byte_width}', copy=False)
        return array_type(schema)(data, self.mask, schema=schema)

    def visit_timestamp(self, schema):
        data = self.data.astype(f'datetime64[{schema.unit}]', copy=False)
        data = data.astype(f'i{schema.byte_width}', copy=False)
        return array_type(schema)(data, self.mask, schema=schema)

    def visit_time(self, schema):
        data = self.data.astype(f'timedelta64[{schema.unit}]', copy=False)
        data = data.astype(f'i{schema.byte_width}', copy=False)
        return array_type(schema)(data, self.mask, schema=schema)

    def visit_opaque(self, schema):
        data = self.data.astype(f'S{schema.byte_width}', copy=False)
        return OpaqueArray(data, self.mask, schema=schema)

    def visit_bytes(self, schema):
        data = b''.join(self.data)
        counts = _make_counts(self.data)
        return BytesArray(data, self.mask, counts=counts)

    def visit_utf8(self, schema):
        values = [v.encode('utf8') for v in self.data]
        data = b''.join(values)
        counts = _make_counts(values)
        return Utf8Array(data, self.mask, counts=counts)

    def visit_list(self, schema):
        if any(isinstance(v, numpy.ma.masked_array) for v in self.data):
            data = numpy.ma.concatenate(self.data)
        else:
            data = numpy.concatenate(self.data)
        data = schema.value.accept(_FromNumpy(data))
        counts = _make_counts(self.data)
        return ListArray(data, self.mask, counts=counts)

    def visit_struct(self, schema):
        data = [(k, v.accept(_FromNumpy(self.data[k])))
                for k, v in schema.fields]
        return StructArray(data, self.mask, schema=schema)


def _numpy_schema(data):
    t = data.dtype.name

    if t == 'bool':
        return Bool()

    if t == 'int8':
        return Int8()

    if t == 'int16':
        return Int16()

    if t == 'int32':
        return Int32()

    if t == 'int64':
        return Int64()

    if t == 'uint8':
        return UInt8()

    if t == 'uint16':
        return UInt16()

    if t == 'uint32':
        return UInt32()

    if t == 'uint64':
        return UInt64()

    if t == 'float16':
        return Float16()

    if t == 'float32':
        return Float32()

    if t == 'float64':
        return Float64()

    if t == 'datetime64[D]':
        return Date('d')

    if t == 'datetime64[s]':
        return Timestamp('s')

    if t == 'datetime64[ms]':
        return Timestamp('ms')

    if t == 'datetime64[us]':
        return Timestamp('us')

    if t == 'datetime64[ns]':
        return Timestamp('ns')

    if t == 'timedelta64[s]':
        return Time('s')

    if t == 'timedelta64[ms]':
        return Time('ms')

    if t == 'timedelta64[us]':
        return Time('us')

    if t == 'timedelta64[ns]':
        return Time('ns')

    if t == 'str':
        return Utf8()

    if t == 'bytes':
        return Bytes()

    if t == 'object':
        if all(v is None for v in data):
            return Null()

        if all(v is None or isinstance(v, str) for v in data):
            return Utf8()

        if all(v is None or isinstance(v, bytes) for v in data):
            return Bytes()

        if all(v is None or isinstance(v, numpy.ndarray) for v in data):
            return List()

    if data.dtype.kind == 'S':
        return Opaque(data.dtype.itemsize)


def to_numpy(array):
    return array.accept(_ToNumpy())


def from_numpy(data, mask=None, *, schema=None):
    if schema is None:
        schema = _numpy_schema(data)

    if schema is None:
        raise ValueError(f'Unable to deduce schema from dtype {data.dtype}')

    return schema.accept(_FromNumpy(data, mask))


def to_pandas(array):
    return pandas.Series(to_numpy(array))


def from_pandas(array, mask=None, *, schema=None):
    return from_numpy(array.values, mask, schema=schema)
