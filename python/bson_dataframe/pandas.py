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

import numpy
import pandas


def _schema_to_pandas(self):
    return self.to_numpy()


def _schema_from_pandas(dtype):
    if isinstance(dtype, numpy.dtype):
        return Schema.from_numpy(dtype)

    assert isinstance(dtype, pandas.api.extensions.ExtensionDtype)
    if dtype.kind in ('i', 'u'):
        return NAMED_SCHEMA[dtype.name.lower()]

    raise ValueError(f'{dtype} is not supported BSON DataFrame type')


class _ArrayToPandas(Visitor):
    def visit_dictionary(self, array):
        index = array.index.to_numpy()
        value = array.value.to_numpy()
        ordered = isinstance(array, OrderedArray)
        values = pandas.Categorical.from_codes(index.data, value.data, ordered)

        return pandas.Series(values)


def _array_to_pandas(self):
    try:
        series = self.accept(_ArrayToPandas())
        assert isinstance(series, pandas.Series)
        return series
    except NotImplementedError:
        pass

    values = self.to_numpy()
    assert isinstance(values, numpy.ma.masked_array)

    if values.dtype.kind in ('f', 'M', 'm', 'O', 'S', 'U'):
        values = values.filled()
    elif values.dtype.kind in ('i', 'u'):
        if values.mask is numpy.ma.nomask:
            values = values.data
        else:
            values = pandas.arrays.IntegerArray(values.data, values.mask)

    return pandas.Series(values)


class _ArrayFromPandas(Visitor):
    def __init__(self, series):
        assert isinstance(series, pandas.Series)

        self.series = series

    def visit_dictionary(self, schema):
        mask = self.series.isna().values
        data = self.series.cat.codes.values
        data = numpy.ma.masked_array(data, mask)
        mask = numpy.packbits(~mask)
        index = Array.from_numpy(data, mask, schema=schema.index)

        value = self.series.cat.categories.values
        value = Array.from_numpy(value, schema=schema.value)

        return array_type(schema)(index, value)


def _array_from_pandas(series, *, schema=None):
    if schema is None:
        schema = _schema_from_pandas(series.dtype)

    try:
        return schema.accept(_ArrayFromPandas(series))
    except NotImplementedError:
        pass

    mask = series.isna().values

    if isinstance(series.dtype, pandas.api.extensions.ExtensionDtype):
        assert series.dtype.kind in ('i', 'u')
        series = series.fillna(0).astype(series.dtype.type)

    assert isinstance(series.dtype, numpy.dtype)
    data = numpy.ma.masked_array(series.values, mask)
    mask = numpy.packbits(~mask)

    return Array.from_numpy(data, mask, schema=schema)


Schema.to_pandas = _schema_to_pandas
Schema.from_pandas = staticmethod(_schema_from_pandas)

Array.to_pandas = _array_to_pandas
Array.from_pandas = staticmethod(_array_from_pandas)
