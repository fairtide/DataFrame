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


def _numpy_name(dtype):
    if dtype.kind == 'U':
        return 'utf8'

    if dtype.kind == 'S':
        if dtype.itemsize == 0:
            return 'bytes'
        else:
            return 'opaque'

    if dtype.kind == 'V':
        if dtype.itemsize == 0:
            return 'null'
        else:
            return 'struct'

    if dtype.kind == 'm':
        return dtype.name.replace('timedelta64', 'time')

    if dtype.kind == 'M':
        return dtype.name.replace('datetime64', 'timestamp')

    return dtype.name


class _SchemaToNumpy(Visitor):
    def visit_null(self, schema):
        return numpy.dtype(object)

    def visit_numeric(self, schema):
        return numpy.dtype(schema.name)

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
        return numpy.dtype(f'S{schema.byte_width}')

    def visit_binary(self, schema):
        return numpy.dtype(object)

    def visit_list(self, schema):
        return numpy.dtype(object)

    def visit_struct(self, schema):
        return numpy.dtype([(k, v.to_numpy()) for k, v in schema.fields])


def schema_to_numpy(self):
    return self.accept(_SchemaToNumpy())


def schema_from_numpy(dtype):
    assert isinstance(dtype, numpy.dtype)

    name = _numpy_name(dtype)

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


def _to_numpy_mask(array):
    if isinstance(array.schema, Struct):
        mdata = [(k, _to_numpy_mask(v)) for k, v in array.fields]
        mtype = numpy.dtype([(k, v.dtype) for k, v in mdata])
        hasmask = False
        mask = numpy.ndarray(len(self), mtype)
        for k, v in mdata:
            haskmask = hasmask and v is not numpy.ma.nomask
            mask[k] = v
        return mask if hasmask else numpy.ma.nomask

    mask = numpy.frombuffer(array.mask, numpy.uint8)

    if len(mask) == 0:
        return numpy.ma.nomask

    if all(mask[:-1] != numpy.uint8(255)):
        bits = numpy.unpackbits(mask[-1:])
        nbits = int(numpy.sum(bits) + len(mask) * 8 - 8)
        if nbits == len(array):
            return numpy.ma.nomask

    mask = numpy.unpackbits(mask).astype(bool, copy=False)
    mask = mask[:len(array)]

    return ~mask


def _to_numpy_array(data, mask, usemask):
    if usemask:
        return numpy.ma.masked_array(data, mask)

    if mask is numpy.ma.nomask:
        return data

    if data.dtype.kind == 'O':
        data[mask] = None
        return data

    if data.dtype.kind == 'f':
        data[mask] = numpy.nan
        return data

    raise ValueError(
        f'{data.dtype} array with nulls can only be converted to numpy.ma.masked_array'
    )


def _from_numpy_mask(array, mask):
    if mask is not None:
        return mask

    if not isinstance(array, numpy.ma.masked_array):
        return

    mask = array.mask
    if not isinstance(mask, numpy.ndarray):
        return

    if mask.dtype.kind != 'b':
        return

    return ~mask


Schema.to_numpy = schema_to_numpy
Schema.from_numpy = staticmethod(schema_from_numpy)
