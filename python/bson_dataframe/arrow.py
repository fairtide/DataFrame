from .schema import *

import bson
import bson.raw_bson
import numpy
import pyarrow


class BaseVisitor(object):
    def accept_type(self, typ, val):
        if pyarrow.types.is_null(typ):
            return self.visit_null(val)

        if pyarrow.types.is_boolean(typ):
            return self.visit_bool(val)

        if pyarrow.types.is_int8(typ):
            return self.visit_int8(val)

        if pyarrow.types.is_int16(typ):
            return self.visit_int16(val)

        if pyarrow.types.is_int32(typ):
            return self.visit_int32(val)

        if pyarrow.types.is_int64(typ):
            return self.visit_int64(val)

        if pyarrow.types.is_uint8(typ):
            return self.visit_uint8(val)

        if pyarrow.types.is_uint16(typ):
            return self.visit_uint16(val)

        if pyarrow.types.is_uint32(typ):
            return self.visit_uint32(val)

        if pyarrow.types.is_uint64(typ):
            return self.visit_uint64(val)

        if pyarrow.types.is_float16(typ):
            return self.visit_float16(val)

        if pyarrow.types.is_float32(typ):
            return self.visit_float32(val)

        if pyarrow.types.is_float64(typ):
            return self.visit_float64(val)

        if pyarrow.types.is_date32(typ):
            return self.visit_date32(val)

        if pyarrow.types.is_date64(typ):
            return self.visit_date64(val)

        if pyarrow.types.is_timestamp(typ):
            return self.visit_timestamp(val)

        if pyarrow.types.is_time32(typ):
            return self.visit_time32(val)

        if pyarrow.types.is_time64(typ):
            return self.visit_time64(val)

        if pyarrow.types.is_string(typ):
            return self.visit_string(val)

        if pyarrow.types.is_binary(typ):
            return self.visit_binary(val)

        if pyarrow.types.is_fixed_size_binary(typ):
            return self.visit_fixed_size_binary(val)

        if pyarrow.types.is_dictionary(typ):
            return self.visit_dictionary(val)

        if pyarrow.types.is_list(typ):
            return self.visit_list(val)

        if pyarrow.types.is_struct(typ):
            return self.visit_struct(val)

        raise NotImplementedError(typ)

    def visit_null(self, val) -> None:
        raise NotImplementedError()

    def visit_bool(self, val) -> None:
        raise NotImplementedError()

    def visit_int8(self, val) -> None:
        raise NotImplementedError()

    def visit_int16(self, val) -> None:
        raise NotImplementedError()

    def visit_int32(self, val) -> None:
        raise NotImplementedError()

    def visit_int64(self, val) -> None:
        raise NotImplementedError()

    def visit_uint8(self, val) -> None:
        raise NotImplementedError()

    def visit_uint16(self, val) -> None:
        raise NotImplementedError()

    def visit_uint32(self, val) -> None:
        raise NotImplementedError()

    def visit_uint64(self, val) -> None:
        raise NotImplementedError()

    def visit_float32(self, val) -> None:
        raise NotImplementedError()

    def visit_float64(self, val) -> None:
        raise NotImplementedError()

    def visit_date32(self, val) -> None:
        raise NotImplementedError()

    def visit_date64(self, val) -> None:
        raise NotImplementedError()

    def visit_string(self, val) -> None:
        raise NotImplementedError()

    def visit_binary(self, val) -> None:
        raise NotImplementedError()

    def visit_timestamp(self, val) -> None:
        raise NotImplementedError()

    def visit_time32(self, val) -> None:
        raise NotImplementedError()

    def visit_time64(self, val) -> None:
        raise NotImplementedError()

    def visit_fixed_size_binary(self, val) -> None:
        raise NotImplementedError()

    def visit_dictionary(self, val) -> None:
        raise NotImplementedError()

    def visit_list(self, val) -> None:
        raise NotImplementedError()

    def visit_struct(self, val) -> None:
        raise NotImplementedError()


class TypeVisitor(BaseVisitor):
    def accept(self, typ: pyarrow.DataType) -> None:
        return self.accept_type(typ, typ)


class ArrayVisitor(BaseVisitor):
    def accept(self, array: pyarrow.Array) -> None:
        return self.accept_type(array.type, array)


class ArrayData(object):
    def __init__(self):
        self.type = None
        self.length = None
        self.null_count = None
        self.buffers = []
        self.children = []
        self.dictionary = None

    def make_array(self):
        if isinstance(self.type, pyarrow.DictionaryType):
            if isinstance(self.dictionary, ArrayData):
                values = self.dictionary.make_array()
            else:
                values = self.dictionary

            return pyarrow.DictionaryArray.from_arrays(
                self._make_array(self.type.index_type),
                values,
                ordered=self.type.ordered)
        else:
            return self._make_array(self.type)

    def _make_array(self, typ):
        return pyarrow.Array.from_buffers(typ,
                                          self.length,
                                          self.buffers,
                                          self.null_count,
                                          children=self.children)


class _TypeWriter(TypeVisitor):
    def __init__(self, doc):
        self.doc = doc

    def _visit_primitive(self, typename):
        self.doc[TYPE] = typename

    def visit_null(self, typ):
        self._visit_primitive('null')

    def visit_bool(self, typ):
        self._visit_primitive('bool')

    def visit_int8(self, typ):
        self._visit_primitive('int8')

    def visit_int16(self, typ):
        self._visit_primitive('int16')

    def visit_int32(self, typ):
        self._visit_primitive('int32')

    def visit_int64(self, typ):
        self._visit_primitive('int64')

    def visit_uint8(self, typ):
        self._visit_primitive('uint8')

    def visit_uint16(self, typ):
        self._visit_primitive('uint16')

    def visit_uint32(self, typ):
        self._visit_primitive('uint32')

    def visit_uint64(self, typ):
        self._visit_primitive('uint64')

    def visit_float16(self, typ):
        self._visit_primitive('float16')

    def visit_float32(self, typ):
        self._visit_primitive('float32')

    def visit_float64(self, typ):
        self._visit_primitive('float64')

    def visit_date32(self, typ):
        self._visit_primitive('date[d]')

    def visit_date64(self, typ):
        self._visit_primitive('date[ms]')

    def visit_binary(self, typ):
        self._visit_primitive('bytes')

    def visit_string(self, typ):
        self._visit_primitive('utf8')

    def visit_timestamp(self, typ):
        assert typ.unit in ('s', 'ms', 'us', 'ns')

        self.doc[TYPE] = f'timestamp[{typ.unit}]'
        if typ.tz is not None:
            self.doc[PARAM] = typ.tz

    def visit_time32(self, typ):
        if typ.equals(pyarrow.time32('s')):
            unit = 's'
        elif typ.equals(pyarrow.time32('ms')):
            unit = 'ms'
        else:
            raise ValueError(f'Unspported type {typ}')

        self.doc[TYPE] = f'time[{unit}]'

    def visit_time64(self, typ):
        if typ.equals(pyarrow.time64('us')):
            unit = 'us'
        elif typ.equals(pyarrow.time64('ns')):
            unit = 'ns'
        else:
            raise ValueError(f'Unspported type {typ}')

        self.doc[TYPE] = f'time[{unit}]'

    def visit_fixed_size_binary(self, typ):
        self.doc[TYPE] = 'opaque'
        self.doc[PARAM] = typ.byte_width

    def visit_list(self, typ):
        param = {}
        _TypeWriter(param).accept(typ.value_type)

        self.doc[TYPE] = 'list'
        self.doc[PARAM] = param

    def visit_struct(self, typ):
        param = []
        for field in typ:
            assert field.name is not None
            assert len(field.name) > 0

            field_doc = {}
            field_doc[NAME] = field.name
            _TypeWriter(field_doc).accept(field.type)
            param.append(field_doc)

        self.doc[TYPE] = 'struct'
        self.doc[PARAM] = param

    def visit_dictionary(self, typ):
        index_doc = {}
        _TypeWriter(index_doc).accept(typ.index_type)

        dict_doc = {}
        _TypeWriter(dict_doc).accept(typ.value_type)

        if typ.ordered:
            self.doc[TYPE] = 'ordered'
        else:
            self.doc[TYPE] = 'factor'

        self.doc[PARAM] = {INDEX: index_doc, DICT: dict_doc}


def write_type(doc, typ: pyarrow.DataType):
    _TypeWriter(doc).accept(typ)

    return doc


def read_type(doc):
    t = doc[TYPE]

    if PARAM in doc:
        tp = doc[PARAM]
    else:
        tp = None

    if t == 'null':
        return pyarrow.null()

    if t == 'bool':
        return pyarrow.bool_()

    if t == 'int8':
        return pyarrow.int8()

    if t == 'int16':
        return pyarrow.int16()

    if t == 'int32':
        return pyarrow.int32()

    if t == 'int64':
        return pyarrow.int64()

    if t == 'uint8':
        return pyarrow.uint8()

    if t == 'uint16':
        return pyarrow.uint16()

    if t == 'uint32':
        return pyarrow.uint32()

    if t == 'uint64':
        return pyarrow.uint64()

    if t == 'float16':
        return pyarrow.float16()

    if t == 'float32':
        return pyarrow.float32()

    if t == 'float64':
        return pyarrow.float64()

    if t == 'date[d]':
        return pyarrow.date32()

    if t == 'date[ms]':
        return pyarrow.date64()

    if t == 'timestamp[s]':
        return pyarrow.timestamp('s')

    if t == 'timestamp[ms]':
        return pyarrow.timestamp('ms')

    if t == 'timestamp[us]':
        return pyarrow.timestamp('us')

    if t == 'timestamp[ns]':
        return pyarrow.timestamp('ns')

    if t == 'time[s]':
        return pyarrow.time32('s')

    if t == 'time[ms]':
        return pyarrow.time32('ms')

    if t == 'time[us]':
        return pyarrow.time64('us')

    if t == 'time[ns]':
        return pyarrow.time64('ns')

    if t == 'utf8':
        return pyarrow.utf8()

    if t == 'bytes':
        return pyarrow.binary()

    if t == 'factor':
        if tp is None:
            index_type = pyarrow.int32()
            dict_type = pyarrow.utf8()
        else:
            index_type = read_type(tp[INDEX])
            dict_type = read_type(tp[DICT])
        return pyarrow.dictionary(index_type, dict_type, False)

    if t == 'ordered':
        if tp is None:
            index_type = pyarrow.int32()
            dict_type = pyarrow.utf8()
        else:
            index_type = read_type(tp[INDEX])
            dict_type = read_type(tp[DICT])
        return pyarrow.dictionary(index_type, dict_type, True)

    if t == 'opaque':
        return pyarrow.binary(tp)

    if t == 'list':
        return pyarrow.list_(read_type(tp))

    if t == 'struct':
        return pyarrow.struct(
            [pyarrow.field(f[NAME], read_type(f)) for f in tp])

    raise ValueError(f'{t} is not supported BSON DataFrame type')


class _DataWriter(ArrayVisitor):
    def __init__(self, doc, compression_level):
        self.doc = doc
        self.compression_level = compression_level

    def _compress(self, data):
        return compress(data, self.compression_level)

    def _make_mask(self, array):
        if isinstance(array, pyarrow.NullArray):
            bitmap = numpy.zeros(len(array), dtype=numpy.bool)
        elif array.null_count == 0:
            bitmap = numpy.ones(len(array), dtype=numpy.bool)
        else:
            begin = array.offset
            end = array.offset + len(array)
            buf = array.buffers()[0]
            val = numpy.ndarray(len(buf), numpy.uint8, buf)
            bitmap = numpy.unpackbits(val, bitorder='little')[begin:end]

        mask = numpy.packbits(bitmap, bitorder='big')
        self.doc[MASK] = self._compress(mask)

    def _make_offsets(self, array):
        t = numpy.int32()
        i = array.offset * t.nbytes
        l = len(array) * t.nbytes + t.nbytes
        buf = array.buffers()[1].slice(i, l)
        n = len(buf) // t.nbytes
        v = numpy.ndarray(n, t, buf)
        u = numpy.diff(v, prepend=v[0])

        return u, v[0], v[-1] - v[0]

    def visit_null(self, array):
        self.doc[DATA] = bson.Int64(len(array))
        self._make_mask(array)
        write_type(self.doc, array.type)

    def visit_bool(self, array):
        begin = array.offset
        end = array.offset + len(array)
        buf = array.buffers()[1]
        val = numpy.ndarray(len(buf), numpy.uint8, buf)
        bitmap = numpy.unpackbits(val, bitorder='little')[begin:end]

        self.doc[DATA] = self._compress(bitmap)
        self._make_mask(array)
        write_type(self.doc, array.type)

    def _visit_numeric(self, array):
        wid = array.type.bit_width // 8

        offset = array.offset * wid
        length = len(array) * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = self._compress(buf)
        self._make_mask(array)
        write_type(self.doc, array.type)

    def visit_int8(self, array):
        self._visit_numeric(array)

    def visit_int16(self, array):
        self._visit_numeric(array)

    def visit_int32(self, array):
        self._visit_numeric(array)

    def visit_int64(self, array):
        self._visit_numeric(array)

    def visit_uint8(self, array):
        self._visit_numeric(array)

    def visit_uint16(self, array):
        self._visit_numeric(array)

    def visit_uint32(self, array):
        self._visit_numeric(array)

    def visit_uint64(self, array):
        self._visit_numeric(array)

    def visit_float16(self, array):
        self._visit_numeric(array)

    def visit_float32(self, array):
        self._visit_numeric(array)

    def visit_float64(self, array):
        self._visit_numeric(array)

    def visit_time32(self, array):
        self._visit_numeric(array)

    def visit_time64(self, array):
        self._visit_numeric(array)

    def _visit_datetime(self, array, dtype):
        wid = array.type.bit_width // 8

        n = len(array)
        i = array.offset * wid
        l = n * wid
        buf = array.buffers()[1].slice(i, l)
        v = numpy.ndarray(n, dtype, buf)
        data = numpy.diff(v, prepend=v.dtype.type(0))

        self.doc[DATA] = self._compress(data)
        self._make_mask(array)
        write_type(self.doc, array.type)

    def visit_date32(self, array):
        self._visit_datetime(array, numpy.int32)

    def visit_date64(self, array):
        self._visit_datetime(array, numpy.int64)

    def visit_timestamp(self, array):
        self._visit_datetime(array, numpy.int64)

    def visit_fixed_size_binary(self, array):
        wid = array.type.byte_width

        offset = array.offset * wid
        length = len(array) * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = self._compress(buf)
        self._make_mask(array)
        write_type(self.doc, array.type)

    def visit_binary(self, array):
        offsets, data_offset, data_length = self._make_offsets(array)

        data = b''
        if data_length != 0:
            data = array.buffers()[2].slice(data_offset, data_length)

        self.doc[DATA] = self._compress(data)
        self._make_mask(array)
        write_type(self.doc, array.type)
        self.doc[OFFSET] = self._compress(offsets)

    def visit_string(self, array):
        self.visit_binary(array)

    def visit_list(self, array):
        offsets, values_offset, values_length = self._make_offsets(array)
        values = array.flatten().slice(values_offset, values_length)

        data = {}
        _DataWriter(data, self.compression_level).accept(values)

        self.doc[DATA] = data
        self._make_mask(array)
        write_type(self.doc, array.type)
        self.doc[OFFSET] = self._compress(offsets)

    def visit_struct(self, array):
        fields = {}
        for i, field in enumerate(array.type):
            assert field.name is not None
            assert len(field.name) > 0

            field_data = array.field(i)
            field_doc = {}
            _DataWriter(field_doc, self.compression_level).accept(field_data)
            fields[field.name] = field_doc

        self.doc[DATA] = {LENGTH: bson.Int64(len(array)), FIELDS: fields}
        self._make_mask(array)
        write_type(self.doc, array.type)

    def visit_dictionary(self, array):
        index_doc = {}
        _DataWriter(index_doc, self.compression_level).accept(array.indices)

        dict_doc = {}
        _DataWriter(dict_doc, self.compression_level).accept(array.dictionary)

        self.doc[DATA] = {INDEX: index_doc, DICT: dict_doc}
        self._make_mask(array)
        write_type(self.doc, array.type)


def write_data(doc, array: pyarrow.Array, compression_level=0):
    _DataWriter(doc, compression_level).accept(array)

    return doc


class _DataReader(TypeVisitor):
    def __init__(self, data, doc):
        self.data = data
        self._doc = doc

    def _decompress(self, data):
        return pyarrow.py_buffer(decompress(data))

    def _make_mask(self):
        assert self.data.length is not None

        if self.data.type.equals(pyarrow.null()):
            self.data.null_count = self.data.length
            return None

        if self.data.length == 0:
            self.data.null_count = 0
            return None

        bits = self._decompress(self._doc[MASK])
        vals = numpy.unpackbits(numpy.ndarray(len(bits), numpy.uint8, bits),
                                bitorder='big')

        self.data.null_count = self.data.length - numpy.sum(vals)
        if self.data.null_count == 0:
            return None

        mask = numpy.packbits(vals, bitorder='little')

        return pyarrow.py_buffer(mask.tobytes())

    def _make_offsets(self):
        t = numpy.int32()
        buf = self._decompress(self._doc[OFFSET])

        assert len(buf) % t.nbytes == 0

        n = len(buf) // t.nbytes
        v = numpy.ndarray(n, t, buf)
        u = numpy.cumsum(v, dtype=v.dtype)

        return pyarrow.py_buffer(u.tobytes()), n - 1

    def visit_null(self, typ):
        self.data.length = self._doc[DATA]
        self.data.buffers.append(self._make_mask())

    def visit_bool(self, typ):
        buf = self._decompress(self._doc[DATA])
        n = len(buf)
        p = numpy.ndarray(n, numpy.uint8, buf)
        bits = numpy.packbits(p, bitorder='little')
        bitmap = pyarrow.py_buffer(bits.tobytes())

        self.data.length = n
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(bitmap)

    def _visit_numeric(self, typ):
        buf = self._decompress(self._doc[DATA])

        assert (len(buf) * 8) % typ.bit_width == 0

        self.data.length = len(buf) * 8 // typ.bit_width
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(buf)

    def visit_int8(self, typ):
        self._visit_numeric(typ)

    def visit_int16(self, typ):
        self._visit_numeric(typ)

    def visit_int32(self, typ):
        self._visit_numeric(typ)

    def visit_int64(self, typ):
        self._visit_numeric(typ)

    def visit_uint8(self, typ):
        self._visit_numeric(typ)

    def visit_uint16(self, typ):
        self._visit_numeric(typ)

    def visit_uint32(self, typ):
        self._visit_numeric(typ)

    def visit_uint64(self, typ):
        self._visit_numeric(typ)

    def visit_float16(self, typ):
        self._visit_numeric(typ)

    def visit_float32(self, typ):
        self._visit_numeric(typ)

    def visit_float64(self, typ):
        self._visit_numeric(typ)

    def visit_time32(self, typ):
        self._visit_numeric(typ)

    def visit_time64(self, typ):
        self._visit_numeric(typ)

    def _visit_datetime(self, typ, dtype):
        wid = typ.bit_width // 8

        buf = self._decompress(self._doc[DATA])

        assert len(buf) % wid == 0

        v = numpy.ndarray(len(buf) // wid, dtype, buf)
        u = numpy.cumsum(v, dtype=v.dtype).tobytes()

        self.data.length = len(v)
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(pyarrow.py_buffer(u))

    def visit_date32(self, typ):
        self._visit_datetime(typ, numpy.int32)

    def visit_date64(self, typ):
        self._visit_datetime(typ, numpy.int64)

    def visit_timestamp(self, typ):
        self._visit_datetime(typ, numpy.int64)

    def visit_binary(self, typ):
        offsets, length = self._make_offsets()

        self.data.length = length
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(offsets)
        self.data.buffers.append(self._decompress(self._doc[DATA]))

    def visit_string(self, typ):
        self.visit_binary(typ)

    def visit_fixed_size_binary(self, typ):
        buf = self._decompress(self._doc[DATA])

        assert len(buf) % typ.byte_width == 0

        self.data.length = len(buf) // typ.byte_width
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(buf)

    def visit_list(self, typ):
        offsets, length = self._make_offsets()

        self.data.length = length
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(offsets)

        data = ArrayData()
        data.type = typ.value_type
        _DataReader(data, self._doc[DATA]).accept(data.type)

        self.data.type = pyarrow.list_(data.type)
        self.data.children.append(data.make_array())

    def visit_struct(self, typ):
        data_doc = self._doc[DATA]

        self.data.length = data_doc[LENGTH]
        self.data.buffers.append(self._make_mask())

        field_docs = data_doc[FIELDS]

        fields = []
        for field in typ:
            field_doc = field_docs[field.name]
            field_data = ArrayData()
            field_data.type = field.type
            _DataReader(field_data, field_doc).accept(field_data.type)

            fields.append(pyarrow.field(field.name, field_data.type))
            self.data.children.append(field_data.make_array())

        self.data.type = pyarrow.struct(fields)

    def visit_dictionary(self, typ):
        data_doc = self._doc[DATA]

        index_doc = data_doc[INDEX]
        self.data.type = read_type(index_doc)
        _DataReader(self.data, index_doc).accept(self.data.type)

        dict_doc = data_doc[DICT]
        self.data.dictionary = ArrayData()
        self.data.dictionary.type = read_type(dict_doc)
        _DataReader(self.data.dictionary,
                    dict_doc).accept(self.data.dictionary.type)

        self.data.type = pyarrow.dictionary(self.data.type,
                                            self.data.dictionary.type,
                                            typ.ordered)


def read_data(doc) -> ArrayData:
    data = ArrayData()
    data.type = read_type(doc)
    _DataReader(data, doc).accept(data.type)

    return data


def write_array(array: pyarrow.Array, compression_level=0) -> bytes:
    doc = write_data({}, array, compression_level)

    return bson.encode(doc)


def write_table(table: pyarrow.Table, compression_level=0) -> bytes:
    table = table.combine_chunks()
    doc = {}
    for col in table.columns:
        buf = write_array(col.data.chunks[0], compression_level)
        doc[col.name] = bson.raw_bson.RawBSONDocument(buf)

    return bson.encode(doc)


def read_array(doc) -> pyarrow.Array:
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    return read_data(doc).make_array()


def read_table(doc) -> pyarrow.Table:
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    data = [pyarrow.column(k, read_array(v)) for k, v in doc.items()]

    return pyarrow.Table.from_arrays(data)
