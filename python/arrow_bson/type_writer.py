from .schema import *
from .visitor import *


class TypeWriter(TypeVisitor):
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
        param = dict()
        writer = TypeWriter(param)
        writer.accept(typ.value_type)

        self.doc[TYPE] = 'list'
        self.doc[PARAM] = param

    def visit_struct(self, typ):
        param = list()
        for field in typ:
            assert field.name is not None
            assert len(field.name) > 0

            field_doc = dict()
            field_doc[NAME] = field.name
            writer = TypeWriter(field_doc)
            writer.accept(field.type)
            param.append(field_doc)

        self.doc[TYPE] = 'struct'
        self.doc[PARAM] = param

    def visit_dictionary(self, typ):
        index_doc = dict()
        index_writer = TypeWriter(index_doc)
        index_writer.accept(typ.index_type)

        dict_doc = dict()
        dict_writer = TypeWriter(dict_doc)
        dict_writer.accept(typ.value_type)

        if typ.ordered:
            self.doc[TYPE] = 'ordered'
        else:
            self.doc[TYPE] = 'factor'

        self.doc[PARAM] = {INDEX: index_doc, DICT: dict_doc}
