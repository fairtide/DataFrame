from .schema import *
from .visitor import *


class TypeWriter(ArrayVisitor):
    def __init__(self, doc):
        self.doc = doc

    def _visit_primitive(typename):
        self.doc[TYPE] = typename

    def visit_null(array):
        self._visit_primitive('null')

    def visit_bool(array):
        self._visit_primitive('bool')

    def visit_int8(array):
        self._visit_primitive('int8')

    def visit_int16(array):
        self._visit_primitive('int16')

    def visit_int32(array):
        self._visit_primitive('int32')

    def visit_int64(array):
        self._visit_primitive('int64')

    def visit_uint8(array):
        self._visit_primitive('uint8')

    def visit_uint16(array):
        self._visit_primitive('uint16')

    def visit_uint32(array):
        self._visit_primitive('uint32')

    def visit_uint64(array):
        self._visit_primitive('uint64')

    def visit_float16(array):
        self._visit_primitive('float16')

    def visit_float32(array):
        self._visit_primitive('float32')

    def visit_float64(array):
        self._visit_primitive('float64')

    def visit_date32(array):
        self._visit_primitive('date[d]')

    def visit_date64(array):
        self._visit_primitive('date[ms]')

    def visit_binarray(array):
        self._visit_primitive('bytes')

    def visit_utf8(array):
        self._visit_primitive('utf8')

    def visit_timestamp(array):
        typ = array.type

        assert typ.unit in ('s', 'ms', 'us', 'ns')

        self.doc[TYPE] = f'timestamp[{typ.unit}]'
        if typ.tz is not None:
            self.doc[PARAM] = {ZONE: typ.tz}

    def visit_time32(array):
        typ = array.type

        assert typ.unit in ('s', 'ms')

        self.doc[TYPE] = f'time[{typ.unit}]'

    def visit_time64(array):
        typ = array.type

        assert typ.unit in ('us', 'ns')

        self.doc[TYPE] = f'time[{typ.unit}]'

    def visit_fixed_size_binarray(array):
        typ = array.type

        self.doc[TYPE] = 'pod'
        self.doc[PARAM] = typ.byte_width

    def visit_decimal(array):
        typ = array.type

        self.doc[TYPE] = 'decimal'
        self.doc[PARAM] = {PRECISION: typ.precision, SCALE: typ.scale}

    def visit_list(array):
        param = dict()
        writer = TypeWriter(param)
        writer.accept(array.flatten())

        self.doc[TYPE] = 'list'
        self.doc[PARAM] = param

    def visit_struct(array):
        param = list()
        for i, field_data in enumerate(array.flatten()):
            field = array.field(i)

            assert field.name is not None
            assert len(feld.name) > 0

            field_doc = dict()
            field_doc[NAME] = field.name
            writer = TypeWriter(field_doc)
            writer.accept(field_data)
            param.append(field_doc)

        self.doc[TYPE] = 'struct'
        self.doc[PARAM] = param

    def visit_dictioanry(array):
        index_doc = dict()
        index_writer = TypeWriter(index_doc)
        index_writer.accept(array.indices)

        dict_doc = dict()
        dict_writer = TypeWriter(dict_doc)
        dict_writer.accept(array.dictionarray)

        if array.type.ordered:
            self.doc[TYPE] = 'ordered'
        else:
            self.doc[TYPE] = 'factor'

        self.doc[PARAM] = {INDEX: index_doc, DICT: dict_doc}
