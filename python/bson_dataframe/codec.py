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

import bson
import bson.raw_bson
import lz4.block
import numpy


class CodecOptions(object):
    def __init__(self, *, document_class=None, compression=0):
        self._document_class = document_class
        self._compression = compression

        if compression == 0:
            self._mode = 'default'
        else:
            self._mode = 'high_compression'

        if document_class is None:
            self._init_class = dict
        elif document_class is bson.raw_bson.RawBSONDocument:
            self._init_class = dict
        else:
            self._init_class = document_class

    @property
    def document_class(self):
        return self._document_class

    @property
    def mode(self):
        return self._mode

    @property
    def compression(self):
        return self._compression

    def empty(self):
        return self._init_class()

    def finish(self, doc):
        assert isinstance(doc, self._init_class)

        if self.document_class is None:
            doc = bson.BSON.encode(doc)
        elif self.document_class is bson.raw_bson.RawBSONDocument:
            doc = bson.BSON.encode(doc)
            doc = bsno.raw_bson.RawBSONDocument(doc)

        return doc


class _EncodeSchema(Visitor):
    def __init__(self, options):
        self.options = options

    def _make_doc(self, schema, param=None):
        doc = self.options.empty()
        doc[TYPE] = schema.name
        if param is not None:
            doc[PARAM] = param
        return doc

    def visit_numeric(self, schema):
        return self._make_doc(schema)

    def visit_date(self, schema):
        return self._make_doc(schema)

    def visit_timestamp(self, schema):
        return self._make_doc(schema)

    def visit_time(self, schema):
        return self._make_doc(schema)

    def visit_opaque(self, schema):
        param = int(schema.byte_width)
        assert 0 < param < (1 << 31)
        return self._make_doc(schema, param)

    def visit_binary(self, schema):
        return self._make_doc(schema)

    def visit_dictionary(self, schema):
        if schema.index == Int32() and schema.value == Utf8():
            param = None
        else:
            param = self.options.empty()
            param[INDEX] = schema.index.accept(self)
            param[VALUE] = schema.value.accept(self)
        return self._make_doc(schema, param)

    def visit_list(self, schema):
        param = schema.value.accept(self)
        return self._make_doc(schema, param)

    def visit_struct(self, schema):
        param = list()
        for k, v in schema.fields.items():
            p = self.options.empty()
            p[NAME] = k
            p.update(v.accept(self))
            param.append(p)
        return self._make_doc(schema, param)


class _EncodeArray(Visitor):
    def __init__(self, options):
        self.options = options
        self.schema_encoder = _EncodeSchema(self.options)

    def _compress(self, data):
        assert isinstance(data, bytes)
        return bson.Binary(
            lz4.block.compress(data,
                               mode=self.options.mode,
                               compression=self.options.compression))

    def _encode_data(self, data, mask):
        doc = self.options.empty()
        doc[DATA] = self._compress(data)
        doc[MASK] = self._compress(mask)
        return doc

    def _encode_datetime(self, array):
        data = numpy.frombuffer(array.data, f'i{array.schema.byte_width}')
        data = numpy.diff(data, prepend=data.dtype.type(0)).tobytes()
        return self._encode_data(data, array.mask)

    def _make_doc(self, doc, array):
        doc.update(array.schema.accept(self.schema_encoder))
        return doc

    def visit_numeric(self, array):
        doc = self._encode_data(array.data, array.mask)
        return self._make_doc(doc, array)

    def visit_date(self, array):
        doc = self._encode_datetime(array)
        return self._make_doc(doc, array)

    def visit_timestamp(self, array):
        doc = self._encode_datetime(array)
        return self._make_doc(doc, array)

    def visit_time(self, array):
        doc = self._encode_data(array.data, array.mask)
        return self._make_doc(doc, array)

    def visit_opaque(self, array):
        doc = self._encode_data(array.data, array.mask)
        return self._make_doc(doc, array)

    def visit_binary(self, array):
        doc = self._encode_data(array.data, array.mask)
        doc[COUNT] = self._compress(array.counts)
        return self._make_doc(doc, array)

    def visit_dictionary(self, array):
        data = self.options.empty()
        data[INDEX] = array.index.accept(self)
        data[VALUE] = array.value.accept(self)

        doc = self.options.empty()
        doc[DATA] = data
        doc[MASK] = self._compress(array.mask)

        return self._make_doc(doc, array)

    def visit_list(self, array):
        doc = self.options.empty()
        doc[DATA] = array.data.accept(self)
        doc[MASK] = self._compress(array.mask)
        doc[COUNT] = self._compress(array.counts)

        return self._make_doc(doc, array)

    def visit_struct(self, array):
        fields = self.options.empty()
        for k, v in array.fields.items():
            fields[k] = v.accept(self)

        data = self.options.empty()
        data[LENGTH] = bson.Int64(len(array))
        data[FIELDS] = fields

        doc = self.options.empty()
        doc[DATA] = data
        doc[MASK] = self._compress(array.mask)

        return self._make_doc(doc, array)


class _DecodeArray(Visitor):
    def __init__(self, doc):
        self.doc = doc

    def _decompress(self, data):
        return lz4.block.decompress(data)

    def _decode_data(self):
        data = self._decompress(self.doc[DATA])
        mask = self._decompress(self.doc[MASK])
        return data, mask

    def _decode_datetime(self, schema):
        data, mask = self._decode_data()
        data = numpy.frombuffer(data, f'i{schema.byte_width}')
        data = numpy.cumsum(data, dtype=data.dtype)
        return data.tobytes(), mask

    def visit_numeric(self, schema):
        atype = array_type(schema)
        return atype(*self._decode_data())

    def visit_date(self, schema):
        atype = array_type(schema)
        return atype(*self._decode_datetime(schema), schema=schema)

    def visit_timestamp(self, schema):
        atype = array_type(schema)
        return atype(*self._decode_datetime(schema), schema=schema)

    def visit_time(self, schema):
        atype = array_type(schema)
        return atype(*self._decode_data(), schema=schema)

    def visit_opaque(self, schema):
        atype = array_type(schema)
        return atype(*self._decode_data(), schema=schema)

    def visit_binary(self, schema):
        atype = array_type(schema)
        counts = self._decompress(self.doc[COUNT])
        length = len(counts) // 4 - 1
        return atype(*self._decode_data(), length=length, counts=counts)

    def visit_dictionary(self, schema):
        atype = array_type(schema)
        index = schema.index.accept(_DecodeArray(self.doc[DATA][INDEX]))
        value = schema.value.accept(_DecodeArray(self.doc[DATA][VALUE]))
        return atype(index, value)

    def visit_list(self, schema):
        atype = array_type(schema)
        data = schema.value.accept(_DecodeArray(self.doc[DATA]))
        mask = self._decompress(self.doc[MASK])
        counts = self._decompress(self.doc[COUNT])
        length = len(counts) // 4 - 1
        return atype(data, mask, length=length, counts=counts)

    def visit_struct(self, schema):
        atype = array_type(schema)
        length = self.doc[DATA][LENGTH]
        data = ((k, v.accept(_DecodeArray(self.doc[DATA][FIELDS][k])))
                for k, v in schema.fields.items())
        mask = self._decompress(self.doc[MASK])
        return atype(data, mask, length=length)


def _encode_schema(self, options=None):
    if options is None:
        options = CodecOptions()
    return options.finish(self.accept(_EncodeSchema(options)))


def _decode_schema(doc):
    '''Decode the schema from BSON document

    Args:
        doc (bytes or dictionary type): BSON document or dictionary to be
            encoded as BSON to be validated

    Returns:
        A ``Schema`` object with one of its derived class
    '''

    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    name = doc[TYPE]
    param = doc.get(PARAM, None)
    sch = NAMED_SCHEMA.get(name)

    if sch is not None and param is None:
        return sch

    if isinstance(sch, Dictionary):
        index = Schema.decode(param[INDEX])
        value = Schema.decode(param[VALUE])
        return type(sch)(index, value)

    if name == 'opaque':
        return Opaque(param)

    if name == 'list':
        return List(Schema.decode(param))

    if name == 'struct':
        return Struct([(v[NAME], Schema.decode(v)) for v in param])

    raise ValueError(f'{name} is not supported BSON DataFrame type')


def _encode_array(self, options=None):
    if options is None:
        options = CodecOptions()
    return options.finish(self.accept(_EncodeArray(options)))


def _decode_array(doc):
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)
    return _decode_schema(doc).accept(_DecodeArray(doc))


def _encode_dfs(self, options=None):
    if options is None:
        options = CodecOptions()

    doc = options.empty()
    for k, v in self.columns.items():
        doc[k] = v.accept(_EncodeSchema(options))

    return options.finish(doc)


def _decode_dfs(doc):
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)
    return DataFrameSchema((k, _decode_schema(v)) for k, v in doc.items())


def _encode_df(self, options=None):
    if options is None:
        options = CodecOptions()

    doc = options.empty()
    for k, v in self.columns.items():
        doc[k] = v.accept(_EncodeArray(options))

    return options.finish(doc)


def _decode_df(doc):
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)
    return DataFrame((k, _decode_array(v)) for k, v in doc.items())


Schema.encode = _encode_schema
Schema.decode = staticmethod(_decode_schema)

Array.encode = _encode_array
Array.decode = staticmethod(_decode_array)

DataFrameSchema.encode = _encode_dfs
DataFrameSchema.decode = staticmethod(_decode_dfs)

DataFrame.encode = _encode_df
DataFrame.decode = staticmethod(_decode_df)
