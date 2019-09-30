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
from .numpy import numpy_schema, _ToNumpy, _FromNumpy

import datetime
import numpy
import pandas


class _ToPandas(_ToNumpy):
    def __init__(self):
        super().__init__(False)

    def visit_dictionary(self, array):
        mask = array.numpy_mask()
        index = array.index.accept(self)
        value = array.value.accept(self)
        index[mask] = index.dtype.type(-1)
        ordered = isinstance(array.schema, Ordered)

        return pandas.Categorical.from_codes(index, value, ordered)


class _FromPandas(_FromNumpy):
    def __init__(self, series, mask):
        self.series = series

        data = series.values

        if mask is None:
            try:
                mask = ~series.isna().values
            except:
                pass

        if mask is None and series.dtype.kind == 'O':
            mask = numpy.zeros(len(series), bool)
            for i, v in enumerate(data):
                mask[i] = v is not None

        if isinstance(data, numpy.ndarray):
            super().__init__(data, mask)
            return

        if data.dtype.kind == 'i':
            data = data.fillna(0).astype(data.dtype.type)
            super().__init__(data, mask)
            return

        if data.dtype.kind == 'u':
            data = data.fillna(0).astype(data.dtype.type)
            super().__init__(data, mask)
            return

    def visit_dictionary(self, schema):
        index = schema.index.accept(
            _FromPandas(self.series.cat.codes, self.series.isna()))
        value = schema.value.accept(
            _FromPandas(self.series.cat.categories.to_series(), None))
        return array_type(schema)(index, value)


def pandas_schema(data):
    if isinstance(data, numpy.ndarray):
        return numpy_schema(data)

    if data.dtype.kind == 'O':
        data = data.copy()
        data[data.isna()] = None

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

    if t == 'datetime64[Y]':
        return Date('d')

    if t == 'datetime64[M]':
        return Date('d')

    if t == 'datetime64[D]':
        return Date('d')

    if t == 'datetime64[h]':
        return Timestamp('s')

    if t == 'datetime64[m]':
        return Timestamp('s')

    if t == 'datetime64[s]':
        return Timestamp('s')

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
        if isinstance(data, numpy.ma.masked_array):
            na = numpy.ma.masked
        else:
            na = None

        if all(v is na for v in data):
            return Null()

        if all(v is na or isinstance(v, bool) for v in data):
            return Bool()

        if all(v is na or isinstance(v, datetime.date) for v in data):
            return Date('d')

        if all(v is na or isinstance(v, str) for v in data):
            return Utf8()

        if all(v is na or isinstance(v, bytes) for v in data):
            return Bytes()

        if all(v is na or getattr(v, '__len__', None) for v in data):
            value = None
            for v in data:
                if v is not na:
                    sch = pandas_schema(v)
                    if sch is None:
                        return
                    if value is None:
                        value = sch
                    elif value != sch:
                        return
            if value is None:
                return
            return List(value)

    if isinstance(getattr(data, 'values', None), pandas.Categorical):
        index = pandas_schema(data.cat.codes)
        value = pandas_schema(data.cat.categories.to_series())
        if data.cat.ordered:
            return Ordered(index, value)
        else:
            return Factor(index, value)

    if data.dtype.kind == 'S':
        return Opaque(data.dtype.itemsize)

    if data.dtype.kind == 'i':
        return globals()[data.dtype.name]()

    if data.dtype.kind == 'u':
        return globals()[data.dtype.name]()

    return numpy_schema(data.values)


def to_pandas(array):
    values = array.accept(_ToPandas())

    if isinstance(values, pandas.Series):
        return values

    if isinstance(values, numpy.ma.masked_array):
        data = values.data
        mask = values.mask

        if data.dtype.kind == 'i':
            values = pandas.arrays.IntegerArray(data, mask)
        elif data.dtype.kind == 'u':
            values = pandas.arrays.IntegerArray(data, mask)
        elif data.dtype.kind == 'f':
            values = data
            values[mask] = numpy.nan
        elif data.dtype.kind == 'M':
            values = data
            values[mask] = 'NaT'
        else:
            raise ValueError(
                f'Nullable {str(array.schema)} is not supported by pandas')

    return pandas.Series(values)


def from_pandas(data, mask=None, *, schema=None):
    assert isinstance(data, pandas.Series)

    if schema is None:
        schema = pandas_schema(data)

    if schema is None:
        raise ValueError(f'Unable to deduce schema from dtype {data.dtype}')

    return schema.accept(_FromPandas(data, mask))
