from .compress import *
from .schema import *
from .type_writer import *
from .visitor import *
import collections
import pyarrow
import bson


class DataWriter(ArrayVisitor):
    def __init__(self, doc, compression_level):
        self.doc = doc
        self.compress_level = compression_level

    def visit_null(array):
        self.doc[DATA] = bson.Int64(array.length)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def visit_bool(array):
        begin = array.offset
        end = array.offset + array.length
        bitmap = numpy.unpackbits(array.buffers()[0],
                                  bitorder='little')[begin:end]

        self.doc[DATA] = compress(bitmap.tobytes(), self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def _visit_numeric(array):
        typ = array.type
        wid = typ.bit_width // 8

        offset = array.offset * wid
        length = array.length * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(buf, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def visit_int8(array):
        self._visit_numeric(array)

    def visit_int16(array):
        self._visit_numeric(array)

    def visit_int32(array):
        self._visit_numeric(array)

    def visit_int64(array):
        self._visit_numeric(array)

    def visit_uint8(array):
        self._visit_numeric(array)

    def visit_uint32(array):
        self._visit_numeric(array)

    def visit_uint64(array):
        self._visit_numeric(array)

    def visit_float16(array):
        self._visit_numeric(array)

    def visit_flaot32(array):
        self._visit_numeric(array)

    def visit_float64(array):
        self._visit_numeric(array)

    def visit_time32(array):
        self._visit_numeric(array)

    def visit_time64(array):
        self._visit_numeric(array)

    def _visit_datetime(array, dtype):
        typ = array.type
        wid = typ.bit_width // 8

        offset = array.offset * wid
        length = array.length * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(encode_datetime(buf), self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def visit_date32(array):
        self._visit_datetime(array, numpy.int32)

    def visit_date64(array):
        self._visit_datetime(array, numpy.int64)

    def visit_timestamp(array):
        self._visit_datetime(array, numpy.int64)

    def visit_fixed_size_binary(array):
        typ = array.type
        wid = typ.byte_width

        offset = array.offset * wid
        length = array.length * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(buf, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def visit_binary(array):
        offsets = array.buffers()[1].slice(array.offset * 4,
                                           array.length * 4 + 4)
        offsets, data_offset, data_length = encode_offset(offsets)

        data = b''
        if data_length != 0:
            data = array.buffers()[2].slice(data_offset, data_length)

        self.doc[DATA] = compress(data, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

        self.doc[OFFSET] = compress(offsets)

    def visit_utf8(array):
        self.visit_binary(array)

    def visit_list(array):
        offsets = array.buffers()[1].slice(array.offset * 4,
                                           array.length * 4 + 4)
        offsets = numpy.ndarray(offset.length // 4, numpy.int32, offsets)

        values_offset = offsets[0]
        values_length = offsets[-1] - data_offset
        values = array.flatten().slice(values_offset, values_length)

        data = collections.OrderedDict()
        writer = DataWriter(data)
        writer.accept(values)

        self.doc[DATA] = data

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

        self.doc[OFFSET] = compress(offsets)

    def visit_struct(array):
        fields = collections.OrderedDict()
        for i, field_data in enumerate(array.flatten()):
            field = array.field(i)

            assert field.name is not None
            assert len(field.name) > 0

            field_doc = collections.OrderedDict()
            writer = DataWriter(field_doc, self.compression_level)
            writer.accept(field_data)
            fields.append({field.name: field_doc})

        self.doc[DATA] = {LENGTH: bson.Int64(array.length), FIELDS: feilds}

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def visit_dictionary(array):
        index_doc = collections.OrderedDict()
        index_writer = DataWriter(index_doc)
        index_writer.accept(array.indices)

        dict_doc = collections.OrderedDict()
        dict_writer = DataWriter(dict_doc)
        dict_writer.accept(array.dictionary)

        self.doc[DATA] = {INDEX: index_doc, DICT: dict_doc}

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array)

    def _maek_mask(array):
        if array.type.equals(pyarrow.null()):
            bitmap = numpy.zeros(array.length, dtype=numpy.bool)
        elif array.null_count == 0:
            bitmap = numpy.ones(array.length, dtype=numpy.bool)
        else:
            begin = array.offset
            end = array.offset + array.length
            bitmap = numpy.unpackbits(array.buffers()[0],
                                      bitorder='little')[begin:end]

        mask = numpy.packbits(bitmap, bitorder='big')
        self.doc[MASK] = compress(mask, self.compression_level)
