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

import datetime
import numpy


def _make_counts(data):
    counts = numpy.ndarray(len(data) + 1, numpy.int32)
    counts[0] = 0
    for i, v in enumerate(data, 1):
        counts[i] = len(v)

    return counts


class _ToNumpy(Visitor):
    def __init__(self, usemask):
        self._usemask = usemask

    def _make_array(self, data, array):
        mask = array.numpy_mask()

        if data.dtype.kind == 'O' and mask is not numpy.ma.nomask:
            data[mask] = None

        if self._usemask:
            return numpy.ma.masked_array(data, mask)

        if isinstance(array.schema, Null):
            return data

        if data.dtype.kind == 'O':
            return data

        if mask is numpy.ma.nomask:
            return data

        return numpy.ma.masked_array(data, mask)

    def visit_null(self, array):
        data = numpy.ndarray(len(array), dtype=object)
        return self._make_array(data, array)

    def visit_numeric(self, array):
        data = numpy.frombuffer(array.data, array.schema.name)
        return self._make_array(data, array)

    def visit_date(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')

        if array.schema.unit == 'd':
            data = data.astype('datetime64[D]', copy=False).astype(object)

        if array.schema.unit == 'ms':
            data = data.astype('datetime64[ms]', copy=False)

        return self._make_array(data, array)

    def visit_timestamp(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')
        data = data.astype(f'datetime64[{array.schema.unit}]', copy=False)
        return self._make_array(data, array)

    def visit_time(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')
        data = data.astype(f'timedelta64[{array.schema.unit}]', copy=False)
        return self._make_array(data, array)

    def visit_opaque(self, array):
        data = numpy.frombuffer(array.data, f'S{array.schema.byte_width}')
        return self._make_array(data, array)

    def visit_bytes(self, array):
        values = array.value
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]]
        return self._make_array(data, array)

    def visit_utf8(self, array):
        values = array.value
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]].decode('utf8')
        return self._make_array(data, array)

    def visit_list(self, array):
        values = array.value.accept(self)
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        data = numpy.ndarray(len(array), dtype=object)
        for i in range(len(array)):
            data[i] = values[offsets[i]:offsets[i + 1]]
        return self._make_array(data, array)

    def visit_struct(self, array):
        fields = [(k, v.accept(self)) for k, v in array.fields]
        data = numpy.ndarray(len(array), [(k, v.dtype) for k, v in fields])
        for k, v in fields:
            data[k] = v
        return self._make_array(data, array)


class _FromNumpy(Visitor):
    def __init__(self, array, mask):
        self.array = array
        self.data = array
        self.mask = mask

        if isinstance(array, numpy.ma.masked_array):
            self.data = array.data
            if self.mask is None:
                if array.mask is numpy.ma.nomask:
                    self.mask = None
                else:
                    if getattr(array.dtype, 'fields', None) is None:
                        self.mask = array.mask
                    else:
                        self.mask = numpy.ones(len(array), bool)
                        for k in array.dtype.fields:
                            self.mask &= array.mask[k]
                    self.mask = ~self.mask

        if self.mask is None:
            self.mask = numpy.ones(len(array), bool)

        assert isinstance(self.data, numpy.ndarray)
        assert isinstance(self.mask, numpy.ndarray)
        assert len(self.data) == len(self.mask)

        if self.data.dtype.kind == 'O':
            for i, v in enumerate(self.data):
                if v is None:
                    self.mask[i] = False

        self.mask = numpy.packbits(self.mask)

    def visit_null(self, schema):
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
        values = [b'' if v is None else v for v in self.data]
        data = b''.join(values)
        counts = _make_counts(values)
        return BytesArray(data, self.mask, counts=counts)

    def visit_utf8(self, schema):
        values = [b'' if v is None else v.encode('utf8') for v in self.data]
        data = b''.join(values)
        counts = _make_counts(values)
        return Utf8Array(data, self.mask, counts=counts)

    def visit_list(self, schema):
        if len(self.data) > 0:
            null = numpy.ndarray(0, self.data[0].dtype)
            values = [null if v is None else v for v in self.data]
        else:
            values = self.data

        if any(isinstance(v, numpy.ma.masked_array) for v in values):
            data = numpy.ma.concatenate(values)
        else:
            data = numpy.concatenate(values)

        data = schema.value.accept(_FromNumpy(data, None))
        counts = _make_counts(values)

        return ListArray(data, self.mask, counts=counts)

    def visit_struct(self, schema):
        data = [(k, v.accept(_FromNumpy(self.array[k], None)))
                for k, v in schema.fields]
        return StructArray(data, self.mask, schema=schema)


def to_numpy(array, usemask=False):
    return array.accept(_ToNumpy(usemask))


def from_numpy(data, mask=None, *, schema=None):
    assert isinstance(data, numpy.ndarray)

    if schema is None:
        schema = Schema.from_numpy(data)

    if schema is None:
        raise ValueError(f'Unable to deduce schema from dtype {data.dtype}')

    return schema.accept(_FromNumpy(data, mask))
