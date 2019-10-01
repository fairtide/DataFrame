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
from .codec import *

import numpy


class _SchemaToNumpy(Visitor):
    def visit_null(self, schema):
        return numpy.dtype(object)

    def visit_numeric(self, schema):
        dtype = numpy.dtype(schema.name)
        assert schema.byte_width == dtype.itemsize
        return dtype

    def visit_date(self, schema):
        if schema.unit == 'd':
            return numpy.dtype('datetime64[D]')

        if schema.unit == 'ms':
            return numpy.dtype('datetime64[ms]')

    def visit_timestamp(self, schema):
        return numpy.dtype(f'datetime64[{schema.unit}]')

    def visit_time(self, schema):
        return numpy.dtype(f'timedelta64[{schema.unit}]')

    def visit_opaque(self, schema):
        dtype = numpy.dtype(f'S{schema.byte_width}')
        assert schema.byte_width == dtype.itemsize
        return dtype

    def visit_binary(self, schema):
        return numpy.dtype(object)

    def visit_list(self, schema):
        return numpy.dtype(object)

    def visit_struct(self, schema):
        return numpy.dtype([(k, v.to_numpy()) for k, v in schema.fields])


def _schema_from_numpy(dtype):
    if dtype.kind == 'U':
        return 'utf8'

    if dtype.kind == 'S':
        return 'bytes' if dtype.itemsize == 0 else 'opaque'

    if dtype.kind == 'V':
        return 'null' if dtype.itemsize == 0 else 'struct'

    if dtype.kind == 'M':
        return dtype.name.replace('datetime64', 'timestamp')

    if dtype.kind == 'm':
        return dtype.name.replace('timedelta64', 'time')

    return dtype.name


class _ArrayToNumpy(Visitor):
    def __init__(self, usemask):
        self.usemask = usemask

    def _make_mask(self, length, mask):
        mask = numpy.frombuffer(mask, numpy.uint8)

        if len(mask) == 0:
            return numpy.ma.nomask

        if all(mask[:-1] != numpy.uint8(255)):
            bits = numpy.unpackbits(mask[-1:])
            nbits = int(numpy.sum(bits) + len(mask) * 8 - 8)
            if nbits == length:
                return numpy.ma.nomask

        mask = numpy.unpackbits(mask).astype(bool, copy=False)
        mask = mask[:length]

        return ~mask

    def _make_array(self, data, mask):
        if self.usemask:
            return numpy.ma.masked_array(data, mask)

        if mask is numpy.ma.nomask:
            return data

        if mask.dtype.kind == 'b' and not mask.any():
            return data

        raise ValueError(
            f'{data.dtype} array with nulls can only be converted to numpy.ma.masked_array'
        )

    def _make_fixed(self, dtype, array):
        data = numpy.frombuffer(array.data, dtype)
        mask = self._make_mask(len(array), array.mask)
        return self._make_array(data, mask)

    def _make_datetime(self, dtype, array):
        data = self._make_fixed(f'i{array.schema.byte_width}', array)
        return data.astype(dtype, copy=False)

    def _split_data(self, array, values):
        offsets = numpy.cumsum(numpy.frombuffer(array.counts, numpy.int32))
        return [values[offsets[i]:offsets[i + 1]] for i in range(len(array))]

    def visit_null(self, array):
        dtype = schema_to_numpy(array.schema)
        data = numpy.ndarray(len(array), dtype)
        mask = numpy.ma.nomask
        return self._make_array(data, mask)

    def visit_numeric(self, array):
        dtype = schema_to_numpy(array.schema)
        return self._make_fixed(dtype, array)

    def visit_date(self, array):
        dtype = schema_to_numpy(array.schema)
        return self._make_datetime(dtype, array)

    def visit_timestamp(self, array):
        dtype = schema_to_numpy(array.schema)
        return self._make_datetime(dtype, array)

    def visit_time(self, array):
        dtype = schema_to_numpy(array.schema)
        return self._make_datetime(dtype, array)

    def visit_opaque(self, array):
        dtype = schema_to_numpy(array.schema)
        return self._make_fixed(dtype, array)

    def visit_bytes(self, array):
        dtype = schema_to_numpy(array.schema)
        data = self._split_data(array, array.value)
        data = numpy.array(data, dtype)
        mask = self._make_mask(len(array), array.mask)
        return self._make_array(data, mask)

    def visit_utf8(self, array):
        dtype = schema_to_numpy(array.schema)
        data = self._split_data(array, array.value)
        data = [v.decode('utf8') for v in data]
        data = numpy.array(data, dtype)
        mask = self._make_mask(len(array), array.mask)
        return self._make_array(data, mask)

    def visit_list(self, array):
        dtype = schema_to_numpy(array.schema)
        data = self._split_data(array, array.value.accept(self))
        data = numpy.array(data, dtype)
        mask = self._make_mask(len(array), array.mask)
        return self._make_array(data, mask)

    def visit_struct(self, array):
        dtype = schema_to_numpy(array.schema)
        fields = [(k, v.accept(self)) for k, v in array.fields]

        data = numpy.ndarray(len(array), dtype)
        for k, v in fields:
            data[k] = v

        if self.usemask:
            mdata = [(k, v.mask) for k, v in fields]
            mtype = numpy.dtype([(k, v.dtype) for k, v in mdata])
            mask = numpy.ndarray(len(array), mtype)
            for k, v in mdata:
                mask[k] = v
        else:
            mask = self._make_mask(len(array), array.mask)

        return self._make_array(data, mask)


class _ArrayFromNumpy(Visitor):
    def __init__(self, data, mask=None):
        assert isinstance(data, numpy.ndarray)

        self.data = data
        self.mask = mask

    def _make_mask(self):
        if self.mask is not None:
            return self.mask

        if not isinstance(self.data, numpy.ma.masked_array):
            return None

        mask = self.data.mask
        if not isinstance(mask, numpy.ndarray):
            return None

        if mask.dtype.kind != 'b':
            return None

        mask = ~mask
        mask = numpy.packbits(mask)

        return mask

    def _make_fixed(self, dtype):
        data = self.data.astype(dtype, copy=False)
        mask = self._make_mask()
        return data, mask

    def _make_datetime(self, schema):
        atype = array_type(schema)
        dtype = schema_to_numpy(schema)
        data = self.data.astype(dtype, copy=False)
        data = data.astype(f'i{schema.byte_width}')
        mask = self._make_mask()
        return atype(data, mask, schema=schema)

    def _bind_data(self, null):
        if isinstance(self.data, numpy.ma.masked_array):
            na = numpy.ma.masked
        else:
            na = None

        return [null if v is na else v for v in self.data]

    def visit_null(self, schema):
        atype = array_type(schema)
        return atype(len(self.data))

    def visit_numeric(self, schema):
        atype = array_type(schema)
        dtype = schema_to_numpy(schema)
        return atype(*self._make_fixed(dtype))

    def visit_date(self, schema):
        return self._make_datetime(schema)

    def visit_timestamp(self, schema):
        return self._make_datetime(schema)

    def visit_time(self, schema):
        return self._make_datetime(schema)

    def visit_opaque(self, schema):
        atype = array_type(schema)
        dtype = schema_to_numpy(schema)
        return atype(*self._make_fixed(dtype), schema=schema)

    def visit_bytes(self, schema):
        atype = array_type(schema)
        values = self._bind_data(b'')
        length = len(values)
        counts = numpy.array([0] + [len(v) for v in values], numpy.int32)
        data = b''.join(values)
        mask = self._make_mask()
        return atype(data, mask, length=length, counts=counts)

    def visit_utf8(self, schema):
        atype = array_type(schema)
        values = self._bind_data('')
        values = [v.encode('utf8') for v in values]
        length = len(values)
        counts = numpy.array([0] + [len(v) for v in values], numpy.int32)
        data = b''.join(values)
        mask = self._make_mask()
        return atype(data, mask, length=length, counts=counts)

    def visit_list(self, schema):
        atype = array_type(schema)

        if len(self.data) > 0:
            null = numpy.ndarray(0, self.data[0].dtype)
        else:
            null = numpy.ndarray(0)

        values = self._bind_data(null)
        length = len(values)
        counts = numpy.array([0] + [len(v) for v in values], numpy.int32)

        if any(isinstance(v, numpy.ma.masked_array) for v in values):
            data = numpy.ma.concatenate(values)
        else:
            data = numpy.concatenate(values)
        data = schema.value.accept(_ArrayFromNumpy(data))

        mask = self._make_mask()

        return atype(data, mask, length=length, counts=counts)

    def visit_struct(self, schema):
        atype = array_type(schema)

        data = ((k, v.accept(_ArrayFromNumpy(self.data[k])))
                for k, v in schema.fields)

        mask = self._make_mask()

        return atype(data)


def schema_to_numpy(self):
    return self.accept(_SchemaToNumpy())


def schema_from_numpy(dtype):
    assert isinstance(dtype, numpy.dtype)

    name = _schema_from_numpy(dtype)

    sch = NAMED_SCHEMA.get(name)
    if sch is not None:
        return sch

    if name == 'opaque':
        return Opaque(dtype.itemsize)

    if name == 'timestamp[D]':
        return Date('d')

    if name == 'struct':
        return Struct(
            (k, schema_from_numpy(v[0])) for k, v in dtype.fields.items())

    raise ValueError(f'{dtype} is not supported BSON DataFrame type')


def array_to_numpy(self, *, usemask=True):
    return self.accept(_ArrayToNumpy(usemask))


def array_from_numpy(data, mask=None, *, schema=None):
    if schema is None:
        schema = schema_from_numpy(data.dtype)
    return schema.accept(_ArrayFromNumpy(data, mask))


Schema.to_numpy = schema_to_numpy
Schema.from_numpy = staticmethod(schema_from_numpy)

Array.to_numpy = array_to_numpy
Array.from_numpy = staticmethod(array_from_numpy)
