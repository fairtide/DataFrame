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

import abc
import numpy


def _tobytes(data):
    if isinstance(data, bytes):
        return data

    if isinstance(data, numpy.ndarray):
        return data.tobytes()

    with memoryview(data) as view:
        return view.tobytes()


def _make_mask(length, mask=None):
    assert length >= 0

    if length == 0:
        return b''

    nbytes = length // 8
    ntails = length % 8
    if ntails != 0:
        nbytes += 1

    if mask is None:
        mask = True

    if isinstance(mask, bool) or isinstance(mask, numpy.bool_):
        if mask:
            mask = ~numpy.zeros(nbytes, numpy.uint8)
        else:
            mask = numpy.zeros(nbytes, numpy.uint8)

        if ntails != 0:
            nzeros = 8 - ntails
            mask[-1] >>= nzeros
            mask[-1] <<= nzeros

    ret = _tobytes(mask)

    assert len(ret) == nbytes
    if ntails != 0:
        vals = numpy.frombuffer(ret[-1:], numpy.uint8)
        bits = numpy.unpackbits(vals)
        assert not bits[ntails:].any()

    return ret


def _make_counts(length, total, counts):
    if isinstance(counts, numpy.ndarray):
        elems = counts.astype(numpy.int32)
        ret = elems.tobytes()
    else:
        ret = _tobytes(counts)
        elems = numpy.frombuffer(ret, numpy.int32)

    assert len(elems) == length + 1
    assert elems[0] == 0
    assert (elems >= 0).all()
    assert numpy.sum(elems) == total

    return ret


def array_type(schema: Schema):
    assert isinstance(schema, Schema)

    return globals()[f'{type(schema).__name__}Array']


class Array(abc.ABC):
    def __init__(self, data, mask=None):
        self._data = _tobytes(data)
        self._length = len(self._data) // self.schema.byte_width
        self._mask = _make_mask(self._length, mask)

    @abc.abstractmethod
    def accept(self, visitor):
        pass

    def __len__(self):
        return self._length

    def __eq__(self, other):
        if self is other:
            return True

        if self.schema != other.schema:
            return False

        if len(self) != len(other):
            return False

        if self.mask != other.mask:
            return False

        if self.data != other.data:
            return False

        return True

    @property
    def data(self):
        return self._data

    @property
    def mask(self):
        return self._mask


class BoolArray(Array):
    schema = Bool()

    def accept(self, visitor):
        return visitor.visit_bool(self)


class Int8Array(Array):
    schema = Int8()

    def accept(self, visitor):
        return visitor.visit_int8(self)


class Int16Array(Array):
    schema = Int16()

    def accept(self, visitor):
        return visitor.visit_int16(self)


class Int32Array(Array):
    schema = Int32()

    def accept(self, visitor):
        return visitor.visit_int32(self)


class Int64Array(Array):
    schema = Int64()

    def accept(self, visitor):
        return visitor.visit_int64(self)


class UInt8Array(Array):
    schema = UInt8()

    def accept(self, visitor):
        return visitor.visit_uint8(self)


class UInt16Array(Array):
    schema = UInt16()

    def accept(self, visitor):
        return visitor.visit_uint16(self)


class UInt32Array(Array):
    schema = UInt32()

    def accept(self, visitor):
        return visitor.visit_uint32(self)


class UInt64Array(Array):
    schema = UInt64()

    def accept(self, visitor):
        return visitor.visit_uint64(self)


class Float16Array(Array):
    schema = Float16()

    def accept(self, visitor):
        return visitor.visit_float16(self)


class Float32Array(Array):
    schema = Float32()

    def accept(self, visitor):
        return visitor.visit_float32(self)


class Float64Array(Array):
    schema = Float64()

    def accept(self, visitor):
        return visitor.visit_float64(self)


class DateArray(Array):
    schema = Date()

    def accept(self, visitor):
        return visitor.visit_date(self)


class TimestampArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Timestamp)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_timestamp(self)

    @property
    def schema(self):
        return self._schema


class TimeArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Time)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_time(self)

    @property
    def schema(self):
        return self._schema


class OpaqueArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Opaque)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_opaque(self)

    @property
    def schema(self):
        return self._schema


class BinaryArray(Array):
    def __init__(self, data, mask=None, *, length=None, counts=None):
        self._data = _tobytes(data)
        self._length = length
        self._counts = _make_counts(length, len(self._data), counts)
        self._mask = _make_mask(self._length, mask)

    def __eq__(self, other):
        if self is other:
            return True

        if self.schema != other.schema:
            return False

        if len(self) != len(other):
            return False

        if self.mask != other.mask:
            return False

        if self.counts != other.counts:
            return False

        if self.data != other.data:
            return False

        return True

    @property
    def value(self):
        return self.data

    @property
    def counts(self):
        return self._counts


class BytesArray(BinaryArray):
    schema = Bytes()

    def accept(self, visitor):
        return visitor.visit_bytes(self)


class Utf8Array(BinaryArray):
    schema = Utf8()

    def accept(self, visitor):
        return visitor.visit_utf8(self)


class DictionaryArray(Array):
    def __init__(self, index: Array, value: Array):
        assert isinstance(index, Array)
        assert isinstance(value, Array)

        self._index = index
        self._value = value

    def __len__(self):
        return len(self._index)

    def __eq__(self, other):
        if self is other:
            return True

        if self.schema != other.schema:
            return False

        if len(self) != len(other):
            return False

        if self.index != other.index:
            return False

        if self.value != other.value:
            return False

        return True

    @property
    def schema(self):
        return self._schema

    @property
    def data(self):
        return self._index.data

    @property
    def mask(self):
        return self._index.mask

    @property
    def index(self):
        return self._index

    @property
    def value(self):
        return self._value


class OrderedArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Ordered(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_ordered(self)


class FactorArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Factor(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_factor(self)


class ListArray(Array):
    def __init__(self, data: Array, mask=None, *, length=None, counts=None):
        assert isinstance(data, Array)
        self._schema = List(data.schema)
        self._data = data
        self._length = length
        self._counts = _make_counts(length, len(self._data), counts)
        self._mask = _make_mask(self._length, mask)

    def __eq__(self, other):
        if self is other:
            return True

        if self.schema != other.schema:
            return False

        if len(self) != len(other):
            return False

        if self.mask != other.mask:
            return False

        if self.counts != other.counts:
            return False

        if self.data != other.data:
            return False

        return True

    def accept(self, visitor):
        return visitor.visit_list(self)

    @property
    def schema(self):
        return self._schema

    @property
    def value(self):
        return self.data

    @property
    def counts(self):
        return self._counts


class StructArray(Array):
    def __init__(self, data, mask=None, *, length=None, schema=None):
        fields = list()
        self._length = length
        self._data = list()
        for k, v in data:
            assert isinstance(k, str)
            assert isinstance(v, Array)
            fields.append((k, v.schema))
            self._data.append((k, v))
            if self._length is None:
                self._length = len(v)
            else:
                assert self._length == len(v)

        if schema is None:
            schema = Struct(fields)
        else:
            fields = {k: v for k, v in fields}
            data = {k: v for k, v in self._data}
            self._data = list()
            assert isinstance(schema, Struct)
            assert len(schema.fields) == len(fields)
            for k, v in schema.fields:
                assert v == fields[k]
                self._data.append((k, data[k]))

        self._schema = schema
        self._mask = _make_mask(self._length, mask)

    def accept(self, visitor):
        return visitor.visit_struct(self)

    @property
    def schema(self):
        return self._schema

    @property
    def fields(self):
        return self.data
