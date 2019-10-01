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


def _pandas_name(dtype):
    if dtype.kind == 'i':
        return dtype.name.lower()

    if dtype.kind == 'u':
        return dtype.name.upper()

    return dtype.name


def schema_to_pandas(self):
    return schema_to_numpy(self)


def schema_from_pandas(dtype):
    if isinstance(dtype, numpy.dtype):
        return schema_from_numpy(dtype)

    assert isinstance(dtype, pandas.api.extensions.ExtensionDtype)

    name = _pandas_name(dtype)

    sch = NAMED_SCHEMA.get(name)
    if sch is not None:
        return sch

    raise ValueError(f'{dtype} is not supported BSON DataFrame type')


# class _ToPandas(_ToNumpy):
#     def __init__(self):
#         super().__init__(False)
#
#     def visit_dictionary(self, array):
#         mask = array.numpy_mask()
#         index = array.index.accept(self).copy()
#         value = array.value.accept(self)
#         index[mask] = index.dtype.type(-1)
#         ordered = isinstance(array.schema, Ordered)
#
#         return pandas.Categorical.from_codes(index, value, ordered)
#
#
# class _FromPandas(_FromNumpy):
#     def __init__(self, series):
#         self.series = series
#
#         if isinstance(self.series.values, pandas.Categorical):
#             return
#
#         data = series.values
#         mask = None
#
#         if mask is None:
#             try:
#                 mask = ~series.isna().values
#             except:
#                 pass
#
#         if mask is None and series.dtype.kind == 'O':
#             mask = numpy.zeros(len(series), bool)
#             for i, v in enumerate(data):
#                 mask[i] = v is not None
#
#         if isinstance(data, numpy.ndarray):
#             super().__init__(data, mask)
#             return
#
#         if data.dtype.kind == 'i':
#             data = data.fillna(0).astype(data.dtype.type)
#             super().__init__(data, mask)
#             return
#
#         if data.dtype.kind == 'u':
#             data = data.fillna(0).astype(data.dtype.type)
#             super().__init__(data, mask)
#             return
#
#     def visit_dictionary(self, schema):
#         index = from_numpy(self.series.cat.codes.values,
#                            ~self.series.isna().values)
#         value = from_numpy(self.series.cat.categories.values)
#         return array_type(schema)(index, value)
#
#
# def to_pandas(array):
#     values = array.accept(_ToPandas())
#
#     if isinstance(values, pandas.Series):
#         return values
#
#     if isinstance(values, numpy.ma.masked_array):
#         data = values.data
#         mask = values.mask
#
#         if data.dtype.kind == 'i':
#             values = pandas.arrays.IntegerArray(data, mask)
#         elif data.dtype.kind == 'u':
#             values = pandas.arrays.IntegerArray(data, mask)
#         elif data.dtype.kind == 'f':
#             values = data
#             values[mask] = numpy.nan
#         elif data.dtype.kind == 'M':
#             values = data
#             values[mask] = 'NaT'
#         else:
#             raise ValueError(
#                 f'Nullable {str(array.schema)} is not supported by pandas')
#
#     return pandas.Series(values)
#
#
# def from_pandas(data, *, schema=None):
#     assert isinstance(data, pandas.Series)
#
#     if schema is None:
#         schema = Schema.from_pandas(data.dtype)
#
#     if schema is None:
#         raise ValueError(f'Unable to deduce schema from dtype {data.dtype}')
#
#     return schema.accept(_FromPandas(data))

Schema.to_pandas = schema_to_pandas
Schema.from_pandas = staticmethod(schema_from_pandas)
