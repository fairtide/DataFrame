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
        self.compression_level = compression_level

    def visit_null(self, array):
        self.doc[DATA] = bson.Int64(len(array))

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

    def visit_bool(self, array):
        begin = array.offset
        end = array.offset + len(array)
        buf = array.buffers()[1]
        val = numpy.ndarray(len(buf), numpy.uint8, buf)
        bitmap = numpy.unpackbits(val, bitorder='little')[begin:end]

        self.doc[DATA] = compress(bitmap.tobytes(), self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

    def _visit_numeric(self, array):
        wid = array.type.bit_width // 8

        offset = array.offset * wid
        length = len(array) * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(buf, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

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

        offset = array.offset * wid
        length = len(array) * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(encode_datetime(buf, dtype),
                                  self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

    def visit_date32(self, array):
        self._visit_datetime(array, numpy.int32())

    def visit_date64(self, array):
        self._visit_datetime(array, numpy.int64())

    def visit_timestamp(self, array):
        self._visit_datetime(array, numpy.int64())

    def visit_fixed_size_binary(self, array):
        wid = array.type.byte_width

        offset = array.offset * wid
        length = len(array) * wid
        buf = array.buffers()[1].slice(offset, length)

        self.doc[DATA] = compress(buf, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

    def visit_binary(self, array):
        offsets = array.buffers()[1].slice(array.offset * 4,
                                           len(array) * 4 + 4)
        offsets, data_offset, data_length = encode_offsets(offsets)

        data = b''
        if data_length != 0:
            data = array.buffers()[2].slice(data_offset, data_length)

        self.doc[DATA] = compress(data, self.compression_level)

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

        self.doc[OFFSET] = compress(offsets, self.compression_level)

    def visit_string(self, array):
        self.visit_binary(array)

    def visit_list(self, array):
        offsets = array.buffers()[1].slice(array.offset * 4,
                                           len(array) * 4 + 4)
        offsets, values_offset, values_length = encode_offsets(offsets)

        values = array.flatten().slice(values_offset, values_length)

        data = collections.OrderedDict()
        writer = DataWriter(data, self.compression_level)
        writer.accept(values)

        self.doc[DATA] = data

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

        self.doc[OFFSET] = compress(offsets, self.compression_level)

    def visit_struct(self, array):
        fields = collections.OrderedDict()
        for i, field in enumerate(array.type):
            assert field.name is not None
            assert len(field.name) > 0

            field_data = array.field(i)
            field_doc = collections.OrderedDict()
            writer = DataWriter(field_doc, self.compression_level)
            writer.accept(field_data)
            fields[field.name] = field_doc

        self.doc[DATA] = {LENGTH: bson.Int64(len(array)), FIELDS: fields}

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

    def visit_dictionary(self, array):
        index_doc = collections.OrderedDict()
        index_writer = DataWriter(index_doc, self.compression_level)
        index_writer.accept(array.indices)

        dict_doc = collections.OrderedDict()
        dict_writer = DataWriter(dict_doc, self.compression_level)
        dict_writer.accept(array.dictionary)

        self.doc[DATA] = {INDEX: index_doc, DICT: dict_doc}

        self._make_mask(array)

        type_writer = TypeWriter(self.doc)
        type_writer.accept(array.type)

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
        self.doc[MASK] = compress(mask, self.compression_level)
