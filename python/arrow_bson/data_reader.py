from .array_data import *
from .compress import *
from .schema import *
from .type_reader import *
from .visitor import *
import numpy
import pyarrow


class DataReader(TypeVisitor):
    def __init__(self, data, doc):
        assert data.type is not None
        assert data.length is None
        assert data.null_count is None
        assert len(data.buffers) == 0
        assert len(data.children) == 0
        assert data.dictionary is None

        self.data = data
        self._doc = doc

    def visit_null(self, typ):
        self.data.length = self._doc[DATA]
        self.data.buffers.append(self._make_mask())

    def visit_bool(self, typ):
        buf = decompress(self._doc[DATA])
        n = len(buf)
        p = numpy.ndarray(n, numpy.uint8, buf)
        bits = numpy.packbits(p, bitorder='little')
        bitmap = pyarrow.py_buffer(bits.tobytes())

        self.data.length = n
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(bitmap)

    def _visit_numeric(self, typ):
        buf = decompress(self._doc[DATA])

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
        buf, length = decode_datetime(decompress(self._doc[DATA]), dtype)

        self.data.length = length
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(buf)

    def visit_date32(self, typ):
        self._visit_datetime(typ, numpy.int32)

    def visit_date64(self, typ):
        self._visit_datetime(typ, numpy.int64)

    def visit_timestamp(self, typ):
        self._visit_datetime(typ, numpy.int64)

    def visit_binary(self, typ):
        offsets, length = decode_offsets(decompress(self._doc[OFFSET]))

        self.data.length = length
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(offsets)
        self.data.buffers.append(decompress(self._doc[DATA]))

    def visit_string(self, typ):
        self.visit_binary(typ)

    def visit_fixed_size_binary(self, typ):
        buf = decompress(self._doc[DATA])

        assert len(buf) % typ.byte_width == 0

        self.data.length = len(buf) // typ.byte_width
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(buf)

    def visit_list(self, typ):
        offsets, length = decode_offsets(decompress(self._doc[OFFSET]))

        self.data.length = length
        self.data.buffers.append(self._make_mask())
        self.data.buffers.append(offsets)

        data = ArrayData()
        data.type = typ.value_type

        reader = DataReader(data, self._doc[DATA])
        reader.accept(data.type)

        self.data.type = pyarrow.list_(data.type)
        self.data.children.append(data.make_array())

    def visit_struct(self, typ):
        data_doc = self._doc[DATA]

        self.data.length = data_doc[LENGTH]
        self.data.buffers.append(self._make_mask())

        field_docs = data_doc[FIELDS]
        fields = list()

        for field in typ:
            field_doc = field_docs[field.name]

            field_data = ArrayData()
            field_data.type = field.type

            reader = DataReader(field_data, field_doc)
            reader.accept(field_data.type)

            fields.append(pyarrow.field(field.name, field_data.type))

            self.data.children.append(field_data.make_array())

        self.data.type = pyarrow.struct(fields)

    def visit_dictionary(self, typ):
        data_doc = self._doc[DATA]

        index_doc = data_doc[INDEX]
        self.data.type = read_type(index_doc)
        index_reader = DataReader(self.data, index_doc)
        index_reader.accept(self.data.type)

        dict_doc = data_doc[DICT]
        self.data.dictionary = ArrayData()
        self.data.dictionary.type = read_type(dict_doc)
        dict_reader = DataReader(self.data.dictionary, dict_doc)
        dict_reader.accept(self.data.dictionary.type)

        self.data.type = pyarrow.dictionary(self.data.type,
                                            self.data.dictionary.type,
                                            typ.ordered)

    def _make_mask(self):
        assert self.data.length is not None

        if self.data.type.equals(pyarrow.null()):
            self.data.null_count = self.data.length
            return None

        if self.data.length == 0:
            self.data.null_count = 0
            return None

        bits = decompress(self._doc[MASK])
        vals = numpy.unpackbits(numpy.ndarray(len(bits), numpy.uint8, bits),
                                bitorder='big')

        self.data.null_count = self.data.length - numpy.sum(vals)
        if self.data.null_count == 0:
            return None

        mask = numpy.packbits(vals, bitorder='little')

        return pyarrow.py_buffer(mask.tobytes())
