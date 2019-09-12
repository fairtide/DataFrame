from .schema import *
from .array import *

import bson
import bson.raw_bson
import lz4.block
import numpy


def compress(data, compression_level=0) -> bson.Binary:
    '''Compress data to BSON binary

    Args:
        data (buffer-like object): Array to be compressed. If ``data`` is
            ``numpy.ndarray``, its underlying buffer (``ndarray.tobytes()``)
            will be compressed, otherwise the object is compressed as-is.
        compression_level (int): Level of compression, ``0`` for default
            compression, values greater than 1 for will use the high compression mode.
    '''

    kwargs = {}
    if compression_level > 0:
        kwargs['mode'] = 'high_compression'
        kwargs['compression'] = compression_level

    with memoryview(data) as view:
        return bson.Binary(lz4.block.compress(view, **kwargs))


def decompress(data: bson.Binary, dtype=None):
    '''Decompress BSON binary to data

    Args:
        data: Buffer data to be decompressed
        dtype: If ``None``, the raw bytes of the decompressed buffer is
            returned. Otherwise a ``numpy.ndarray`` with the given type and
            decompressed data as underlying buffer is returned.
    '''

    buf = lz4.block.decompress(data)
    if dtype is None:
        return buf

    wid = numpy.dtype(dtype).itemsize
    assert len(buf) % wid == 0

    return numpy.ndarray(len(buf) // wid, dtype, buf)


class _ArrayEncoder(Visitor):
    def __init__(self, compression_level=0):
        self._compression_level = compression_level

    def _compress(self, data):
        return compress(data, self._compression_level)

    def visit_null(self, array):
        return {DATA: bson.Int64(len(array))}

    def visit_numeric(self, array):
        return {DATA: self._compress(array.data)}

    def visit_date(self, array):
        dtype = f'i{array.schema.byte_width}'
        data = numpy.ndarray(len(array), dtype, data)
        data = numpy.diff(data, prepend=data.dtype.type(0))

        return {DATA: self._compress(data)}

    def visit_timestamp(self, array):
        return self.visit_date(array)

    def visit_time(self, array):
        return self.visit_numeric(obj)

    def visit_opaque(self, array):
        return self.visit_numeric(obj)

    def visit_binary(self, array):
        return {
            DATA: self._compress(array.values),
            COUNT: self._compress(array.counts)
        }

    def visit_dictionary(self, array):
        return {
            DATA: {
                INDEX: array.index.accept(self),
                DICT: array.value.accept(self)
            }
        }

    def visit_list(self, array):
        return {
            DATA: array.values.accept(self),
            COUNT: self._compress(array.counts)
        }

    def visit_struct(self, array):
        return {
            DATA: {
                LENGTH: bson.Int64(len(array)),
                FIELDS: dict({k: v.accept(self)
                              for k, v in array.fields})
            }
        }


class _ArrayDecoder(Visitor):
    def __init__(doc):
        self.doc = doc
        self.data = doc[DATA]
        self.mask = decompress(doc[MASK])

    def visit_null(self, schema):
        return NullData(self.data)

    def visit_numeric(self, schema):
        data = decompress(self.data, schema.name)
        dataclass = globals()[schema.name.title() + 'Data']

        return dataclass(data, self.mask)

    def visit_date(self, schema):
        dtype = f'i{schema.byte_width}'
        data = decompress(self.data)
        data = numpy.ndarray(len(data) // schema.byte_width, dtype, data)
        data = numpy.cumsum(data, dtype=dtype)

        return DateData(data, self.mask, schema=schema)

    def visit_timestamp(self, schema):
        dtype = f'i{schema.byte_width}'
        data = decompress(self.data)
        data = numpy.ndarray(len(data) // schema.byte_width, dtype, data)
        data = numpy.cumsum(data, dtype=dtype)

        return TimestampData(data, self.mask, schema=schema)

    def visit_time(self, schema):
        dtype = f'i{schema.byte_width}'
        data = decompress(self.data, dtype)

        return TimeData(data, self.mask, schema=schema)

    def visit_opaque(self, schema):
        dtype = f'S{schema.byte_width}'
        data = decompress(self.data, dtype)

        return OpaqueData(data, self.mask, schema=schema)

    def visit_binary(self, schema):
        values = decompress(self.data)
        counts = decompress(self.doc[COUNT], numpy.int32)
        dataclass = globals()[schema.name.title() + 'Data']

        return dataclass(values, self.mask, counts=counts)

    def visit_dictionary(self, schema):
        index_decoder = _ArrayDecoder(self.data[INDEX])
        value_decoder = _ArrayDecoder(self.data[DICT])
        index = schema.index_type.accept(index_decoder)
        value = schema.value_type.accept(value_decoder)
        dataclass = globals()[schema.name.title() + 'Data']

        return dataclass(index, value)

    def visit_list(self, schema):
        value_decoder = _ArrayDecoder(self.data)
        value = schema.value_type.accept(value_decoder)
        counts = decompress(self.doc[COUNT], numpy.int32)

        return ListData(value, mask, counts=counts)

    def visit_struct(self, schema):
        fields = list()
        for k, v in schema.fields:
            decoder = _ArrayDecoder(self.data[FIELDS][k])
            data = v.accept(decoder)
            fields.append((k, data))

        return StructData(fields)


def encode_array(array: Array, compression_level):
    encoder = _ArrayEncoder(compression_level)
    doc = array.accept(encoder)
    doc.update(array.schema.encode())

    return bson.BSON.encode(doc)


def decode_array(doc):
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    schema = Schema.decode(doc)
    decoder = _ArrayDecoder(doc)

    return schema.accept(decoder)
