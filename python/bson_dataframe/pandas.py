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
from .numpy import *
from .numpy import _ArrayToNumpy, _ArrayFromNumpy

import numpy
import pandas


def _schema_from_numpy(dtype):
    if dtype.kind == 'i':
        return dtype.name.lower()

    if dtype.kind == 'u':
        return dtype.name.upper()

    return dtype.name


class _ArrayToPandas(_ArrayToNumpy):
    def __init__(self):
        super().__init__(True)

    def visit_dictionary(self, array):
        index = array.index.accept(self)
        value = array.value.accept(self)
        ordered = isinstance(array.schema, Ordered)
        return pandas.Categorical.from_codes(index.data, value.data, ordered)


class _ArrayFromPandas(_ArrayFromNumpy):
    def __init__(self, data):
        assert isinstance(data, pandas.Series)

        if isinstance(data.values, pandas.Categorical):
            self.data = data
            self.mask = None
            return

        mask = data.isna().values

        if isinstance(data.dtype, pandas.api.extensions.ExtensionDtype):
            assert data.dtype.kind in ('i', 'u')
            data = data.fillna(0).astype(data.dtype.type)

        assert isinstance(data.dtype, numpy.dtype)
        data = numpy.ma.masked_array(data.values, mask)
        mask = numpy.packbits(~mask)

        super().__init__(data, mask)

    def visit_dictionary(self, schema):
        mask = self.data.isna().values
        data = self.data.cat.codes.values
        data = numpy.ma.masked_array(data, mask)
        mask = numpy.packbits(~mask)

        index = array_from_numpy(data, mask, schema=schema.index)

        value = self.data.cat.categories.values
        value = array_from_numpy(value, schema=schema.value)

        return array_type(schema)(index, value)


def array_to_pandas(array):
    values = array.accept(_ArrayToPandas())

    if isinstance(values, pandas.Series):
        return values

    if isinstance(values, numpy.ma.masked_array):
        if values.dtype.kind in ('f', 'M', 'm', 'O', 'S', 'U'):
            values = values.filled()
        elif values.dtype.kind in ('i', 'u'):
            values = pandas.arrays.IntegerArray(values.data, values.mask)

    return pandas.Series(values)


def array_from_pandas(data, *, schema=None):
    if schema is None:
        schema = schema_from_pandas(data.dtype)
    return schema.accept(_ArrayFromPandas(data))


def schema_to_pandas(self):
    return schema_to_numpy(self)


def schema_from_pandas(dtype):
    if isinstance(dtype, numpy.dtype):
        return schema_from_numpy(dtype)

    assert isinstance(dtype, pandas.api.extensions.ExtensionDtype)

    name = _schema_from_numpy(dtype)

    sch = NAMED_SCHEMA.get(name)
    if sch is not None:
        return sch

    raise ValueError(f'{dtype} is not supported BSON DataFrame type')


Schema.to_pandas = schema_to_pandas
Schema.from_pandas = staticmethod(schema_from_pandas)

Array.to_pandas = array_to_pandas
Array.from_pandas = staticmethod(array_from_pandas)
