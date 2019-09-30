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


def to_pandas(array):
    return pandas.Series(to_numpy(array))


def from_pandas(series, mask=None, *, schema=None):
    data = series.values
    if mask is None and not isinstance(data, numpy.na.masked_array):
        mask = series.notna().values
    return from_numpy(data, mask, schema=schema)
