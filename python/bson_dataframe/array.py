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
import bson
import lz4.block
import numpy


def array_type(schema: Schema):
    assert isinstance(schema, Schema)

    return globals()[f'{type(schema).__name__}Array']


def _compress(data, compression):
    assert isinstance(data, bytes)

    if compression == 0:
        mode = 'default'
    else:
        mode = 'high_compression'

    return bson.Binary(
        lz4.block.compress(data, mode=mode, compression=compression))


def _decompress(data):
    return lz4.block.decompress(data)


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
            mask = numpy.repeat(numpy.uint8(255), nbytes)
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
        assert not any(bits[ntails:])

    return ret


def _make_counts(value, counts):
    if isinstance(counts, numpy.ndarray):
        elems = counts.astype(numpy.int32)
        ret = elems.tobytes()
    else:
        ret = _tobytes(counts)
        elems = numpy.frombuffer(ret, numpy.int32)

    assert elems[0] == 0
    assert numpy.sum(elems) == len(value)
    assert all(elems >= 0)

    return len(elems) - 1, ret


def _encode_data(data, mask, compression):
    return {
        DATA: _compress(data, compression),
        MASK: _compress(mask, compression)
    }


def _decode_data(doc):
    return _decompress(doc[DATA]), _decompress(doc[MASK])


def _encode_datetime(obj, compression):
    data = numpy.frombuffer(obj.data, f'i{obj.schema.byte_width}')
    data = numpy.diff(data, prepend=data.dtype.type(0)).tobytes()
    return _encode_data(data, obj.mask, compression)


def _decode_datetime(schema, doc):
    data, mask = _decode_data(doc)
    data = numpy.frombuffer(data, f'i{schema.byte_width}')
    data = numpy.cumsum(data, dtype=data.dtype)
    return data.tobytes(), mask


class Array(abc.ABC):
    def __init__(self, data, mask=None):
        self._data = _tobytes(data)
        self._length = len(self._data) // self.schema.byte_width
        self._mask = _make_mask(self._length, mask)

    @abc.abstractmethod
    def accept(self, visitor):
        pass

    def encode(self, compression=0):
        doc = self._encode_array(compression)
        doc.update(self.schema.encode())

        return doc

    @staticmethod
    def decode(doc):
        schema = Schema.decode(doc)
        ret = array_type(schema)._decode_array(schema, doc)

        assert ret.schema == schema

        return ret

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

        if getattr(self.schema, 'byte_width', None):
            w = self.schema.byte_width
            if w in (1, 2, 4, 8):
                t = f'i{w}'
            else:
                t = f'S{w}'

            mask1 = numpy.frombuffer(self.mask, numpy.uint8)
            mask2 = numpy.frombuffer(other.mask, numpy.uint8)
            mask1 = numpy.unpackbits(mask1)[:len(self)]
            mask2 = numpy.unpackbits(mask2)[:len(self)]
            mask1 = mask1.astype(bool, copy=False)
            mask2 = mask2.astype(bool, copy=False)

            data1 = numpy.frombuffer(self.data, t)
            data2 = numpy.frombuffer(other.data, t)

            return all(data1[mask1] == data2[mask2])

        if self.data != other.data:
            return False

        return True

    @property
    def data(self):
        return self._data

    @property
    def mask(self):
        return self._mask

    def numpy_mask(self):
        mask = numpy.frombuffer(self.mask, numpy.uint8)
        if self._all_valid(mask):
            return numpy.ma.nomask

        mask = numpy.unpackbits(mask).astype(bool, copy=False)
        mask = mask[:len(self)]

        return ~mask

    def _encode_array(self, compression):
        return _encode_data(self.data, self.mask, compression)

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_data(doc))

    def _all_valid(self, mask):
        if len(mask) == 0:
            return True

        if any(mask[:-1] != numpy.uint8(255)):
            return False

        bits = numpy.unpackbits(mask[-1:])
        nbits = int(numpy.sum(bits) + len(mask) * 8 - 8)

        return nbits == len(self)


class NullArray(Array):
    schema = Null()

    def __init__(self, length):
        self._length = int(length)
        self._data = None
        self._mask = _make_mask(length, False)

    def accept(self, visitor):
        return visitor.visit_null(self)

    def _encode_array(self, compression):
        return {
            DATA: bson.Int64(len(self)),
            MASK: _compress(self.mask, compression)
        }

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(doc[DATA])


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
    def __init__(self, data, mask=None, *, schema=None):
        assert isinstance(schema, Date)
        self._schema = schema
        super().__init__(data, mask)

    def accept(self, visitor):
        return visitor.visit_date(self)

    @property
    def schema(self):
        return self._schema

    def _encode_array(self, compression):
        return _encode_datetime(self, compression)

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_datetime(schema, doc), schema=schema)


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

    def _encode_array(self, compression):
        return _encode_datetime(self, compression)

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_datetime(schema, doc), schema=schema)


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

    def _encode_array(self, compression):
        return _encode_data(self.data, self.mask, compression)

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_data(doc), schema=schema)


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

    def _encode_array(self, compression):
        return _encode_data(self.data, self.mask, compression)

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_data(doc), schema=schema)


class BinaryArray(Array):
    def __init__(self, data, mask=None, *, counts=None):
        self._data = _tobytes(data)
        self._length, self._counts = _make_counts(self._data, counts)
        self._mask = _make_mask(self._length, mask)

    @property
    def value(self):
        return self.data

    @property
    def counts(self):
        return self._counts

    def _encode_array(self, compression):
        ret = _encode_data(self.data, self.mask, compression)
        ret[COUNT] = _compress(self.counts, compression)

        return ret

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(*_decode_data(doc), counts=_decompress(doc[COUNT]))


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
        raise NotImplementedError()

    @property
    def mask(self):
        return self._index.mask

    @property
    def index(self):
        return self._index

    @property
    def value(self):
        return self._value

    def _encode_array(self, compression):
        return {
            DATA: {
                INDEX: self.index.encode(compression),
                VALUE: self.value.encode(compression)
            },
            MASK: _compress(self.mask, compression)
        }

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(Array.decode(doc[DATA][INDEX]),
                   Array.decode(doc[DATA][VALUE]))


class OrderedArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Ordered(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_list(self)


class FactorArray(DictionaryArray):
    def __init__(self, index: Array, value: Array):
        self._schema = Factor(index.schema, value.schema)
        super().__init__(index, value)

    def accept(self, visitor):
        return visitor.visit_factor(self)


class ListArray(Array):
    def __init__(self, data: Array, mask=None, *, counts=None):
        assert isinstance(data, Array)

        self._schema = List(data.schema)
        self._data = data
        self._length, self._counts = _make_counts(self._data, counts)
        self._mask = _make_mask(self._length, mask)

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

    def _encode_array(self, compression):
        return {
            DATA: self.data.encode(compression),
            MASK: _compress(self.mask, compression),
            COUNT: _compress(self.counts, compression)
        }

    @classmethod
    def _decode_array(cls, schema, doc):
        return cls(Array.decode(doc[DATA]),
                   _decompress(doc[MASK]),
                   counts=_decompress(doc[COUNT]))


class StructArray(Array):
    def __init__(self, data, mask=None, *, schema=None):
        fields = list()
        self._length = None
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

    def numpy_mask(self):
        smask = super().numpy_mask()
        fmask = [(k, v.numpy_mask()) for k, v in self.fields]

        if smask is numpy.ma.nomask:
            if all(v is numpy.ma.nomask for _, v in fmask):
                return numpy.ma.nomask

        mask = numpy.ndarray(len(self), [(k, bool) for k, _ in self.fields])
        for k, v in fmask:
            mask[k] = smask | v

        return mask

    def _encode_array(self, compression):
        data = {k: v.encode(compression) for k, v in self.data}

        return {
            DATA: {
                LENGTH: bson.Int64(len(self)),
                FIELDS: data
            },
            MASK: _compress(self.mask, compression)
        }

    @classmethod
    def _decode_array(cls, schema, doc):
        data = ((k, Array.decode(v)) for k, v in doc[DATA][FIELDS].items())
        ret = cls(data, _decompress(doc[MASK]), schema=schema)

        assert len(ret) == doc[DATA][LENGTH]

        return ret
