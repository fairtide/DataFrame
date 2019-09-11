import bson
import bson.json_util
import bson.raw_bson
import json
import jsonschema
import lz4.block
import numpy

DATA = 'd'
MASK = 'm'
TYPE = 't'
PARAM = 'p'

# binary/string/list
OFFSET = 'o'

# list
LENGTH = 'l'

# struct
NAME = 'n'
FIELDS = 'f'

# dictionary
INDEX = 'i'
DICT = 'd'


def compress(data, compression_level=0):
    if isinstance(data, numpy.ndarray):
        data = data.tobytes()

    if compression_level > 0:
        buf = lz4.block.compress(data,
                                 mode='high_compression',
                                 compression=compression_level)
    else:
        buf = lz4.block.compress(data)

    return bson.Binary(buf)


def decompress(data, dtype=None):
    buf = lz4.block.decompress(data)
    if dtype is None:
        return buf

    wid = numpy.dtype(dtype).itemsize
    assert len(buf) % wid == 0

    return numpy.ndarray(len(buf) // wid, dtype, buf)


class SchemaTypes(object):
    @staticmethod
    def const(value):
        return {'enum': [value]}


class BSONTypes(SchemaTypes):
    @staticmethod
    def int32():
        return {'bsonType': 'int'}

    @staticmethod
    def int64():
        return {'bsonType': 'long'}

    @staticmethod
    def binary():
        return {'bsonType': 'binData'}


class CanonicalJSONTypes(SchemaTypes):
    @staticmethod
    def int32():
        return {
            'type': 'object',
            'required': ['$numberInt'],
            'additionalProperties': False,
            'properties': {
                '$numberInt': {
                    'type': 'string'
                }
            }
        }

    @staticmethod
    def int64():
        return {
            'type': 'object',
            'required': ['$numberLong'],
            'additionalProperties': False,
            'properties': {
                '$numberLong': {
                    'type': 'string'
                }
            }
        }

    @staticmethod
    def binary():
        return {
            'type': 'object',
            'required': ['$binary'],
            'additionalProperties': False,
            'properties': {
                '$binary': {
                    'type': 'object',
                    'required': ['base64', 'subType'],
                    'additionalProperties': False,
                    'properties': {
                        'base64': {
                            'type': 'string'
                        },
                        'subType': {
                            'const': '00'
                        }
                    }
                }
            }
        }


class RelaxedJSONTypes(SchemaTypes):
    @staticmethod
    def int32():
        return {'type': 'integer'}

    @staticmethod
    def int64():
        return {'type': 'integer'}

    @staticmethod
    def binary():
        return CanonicalJSONTypes.binary()


class Schema(object):
    def array_schema(self, types=BSONTypes):
        raise NotImplementedError()

    def type_schema(self, types=BSONTypes):
        sch = self.array_schema(types)

        ret = {
            'type': 'object',
            'required': [TYPE],
            'additionalProperties': False,
            'properties': {
                TYPE: sch['properties'][TYPE]
            }
        }

        if PARAM in sch['required']:
            ret['required'].append(PARAM)

        if PARAM in sch['properties']:
            ret['properties'][PARAM] = sch['properties'][PARAM]

        return ret

    def validate(self, doc, validate_data=False):
        if isinstance(doc, bytes):
            doc = bson.raw_bson.RawBSONDocument(doc)

        json_mode = bson.json_util.JSONMode.CANONICAL
        json_options = bson.json_util.JSONOptions(json_mode=json_mode)
        json_doc = bson.json_util.dumps(doc, json_options=json_options)

        jsonschema.validate(instance=json.loads(json_doc),
                            schema=self.array_schema(CanonicalJSONTypes))

        if not validate_data:
            return

        length = self.validate_data(doc)

        bits = lz4.block.decompress(doc[MASK])

        expected = length // 8
        if length % 8 != 0:
            expected += 1

        if len(bits) != expected:
            raise ValueError(
                f'{self.name} mask length {len(bits)} differs from expected {expected}'
            )

    def validate_data(self, doc):
        raise NotImplementedError()


class Null(Schema):
    name = 'null'

    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE],
            'additionalProperties': False,
            'properties': {
                DATA: types.int64(),
                MASK: types.binary(),
                TYPE: types.const(self.name)
            }
        }

    def validate_data(self, doc):
        return int(doc[DATA])


class Numeric(Schema):
    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE],
            'additionalProperties': False,
            'properties': {
                DATA: types.binary(),
                MASK: types.binary(),
                TYPE: types.const(self.name)
            }
        }

    def validate_data(self, doc):
        buf = lz4.block.decompress(doc[DATA])

        if len(buf) % self.byte_width != 0:
            raise ValueError(
                f'{self.name} buffer is not a multiple of byte width {self.byte_width}'
            )

        return len(buf) // self.byte_width


class Bool(Numeric):
    name = 'bool'
    byte_width = 1


class Int8(Numeric):
    name = 'int8'
    byte_width = 1


class Int16(Numeric):
    name = 'int16'
    byte_width = 2


class Int32(Numeric):
    name = 'int32'
    byte_width = 4


class Int64(Numeric):
    name = 'int64'
    byte_width = 8


class UInt8(Numeric):
    name = 'uint8'
    byte_width = 1


class UInt16(Numeric):
    name = 'uint16'
    byte_width = 2


class UInt32(Numeric):
    name = 'uint32'
    byte_width = 4


class UInt64(Numeric):
    name = 'uint64'
    byte_width = 8


class Float16(Numeric):
    name = 'float16'
    byte_width = 2


class Float32(Numeric):
    name = 'float32'
    byte_width = 4


class Float64(Numeric):
    name = 'float64'
    byte_width = 8


class Date(Numeric):
    def __init__(self, unit):
        if unit not in ('d', 'ms'):
            raise ValueError(f'{self.name} invalid unit {unit}')

        self.name = f'date[{unit}]'

        if unit == 'd':
            self.byte_width = 4

        if unit == 'ms':
            self.byte_width = 8


class Timestamp(Numeric):
    byte_width = 8

    def __init__(self, unit, tz=None):
        if unit not in ('s', 'ms', 'us', 'ns'):
            raise ValueError(f'{self.name} invalid unit {unit}')

        self.name = f'timestamp[{unit}]'
        self.tz = tz

    def array_schema(self, types=BSONTypes):
        ret = super().array_schema(types)
        ret['properties'][PARAM] = {'type': 'string'}

        return ret


class Time(Numeric):
    def __init__(self, unit):
        if unit not in ('s', 'ms', 'us', 'ns'):
            raise ValueError(f'{self.name} invalid unit {unit}')

        self.name = f'time[{unit}]'

        if unit == 's' or unit == 'ms':
            self.byte_width = 4

        if unit == 'us' or unit == 'ns':
            self.byte_width = 8


class Binary(Schema):
    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, OFFSET],
            'additionalProperties': False,
            'properties': {
                DATA: types.binary(),
                MASK: types.binary(),
                TYPE: types.const(self.name),
                OFFSET: types.binary()
            }
        }

    def validate_data(self, doc):
        values = lz4.block.decompress(doc[DATA])

        offsets = lz4.block.decompress(doc[OFFSET])
        if len(offsets) % 4 != 0:
            raise ValueError(
                'Offsets buffer is not a multiple of byte width 4')

        counts = numpy.ndarray(len(offsets) // 4, numpy.int32, offsets)
        if not all(counts >= 0):
            raise ValueError(
                f'{self.name} offsets differences are not all non-negative')

        total = numpy.sum(counts)
        if total != len(values):
            raise ValueError(
                f'{self.name} length of values {len(values)} is different from the last offset {total}'
            )

        return len(counts) - 1


class Bytes(Binary):
    name = 'bytes'


class Utf8(Binary):
    name = 'utf8'


class Opaque(Schema):
    name = 'opaque'

    def __init__(self, byte_width):
        self.byte_width = byte_width

    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, PARAM],
            'additionalProperties': False,
            'properties': {
                DATA: types.binary(),
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: types.int32()
            }
        }

    def validate_data(self, doc):
        values = lz4.block.decompress(doc[DATA])

        byte_width = int(doc[PARAM])
        if byte_width != self.byte_width:
            raise ValueError(
                f'Expected byte width {self.byte_width} differs from that in data {byte_width}'
            )

        if len(values) % byte_width != 0:
            raise ValueError(
                f'{self.name} buffer is not a multiple of byte width {byte_width}'
            )

        return len(values) // byte_width


class Dictionary(Schema):
    def __init__(self, index_type, value_type, ordered):
        self.index_type = index_type
        self.value_type = value_type
        self.name = 'ordered' if ordered else 'factor'

    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE],
            'additionalProperties': False,
            'properties': {
                DATA: {
                    'type': 'object',
                    'required': [INDEX, DICT],
                    'additionalProperties': False,
                    'properties': {
                        INDEX: self.index_type.array_schema(types),
                        DICT: self.value_type.array_schema(types)
                    }
                },
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: {
                    'type': 'object',
                    'required': [INDEX, DICT],
                    'additionalProperties': False,
                    'properties': {
                        INDEX: self.index_type.type_schema(types),
                        DICT: self.value_type.type_schema(types)
                    }
                }
            }
        }

    def validate_data(self, doc):
        self.value_type.validate_data(doc[DATA][DICT])

        return self.index_type.validate_data(doc[DATA][INDEX])


class List(Schema):
    name = 'list'

    def __init__(self, value_type):
        self.value_type = value_type

    def array_schema(self, types=BSONTypes):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, PARAM, OFFSET],
            'additionalProperties': False,
            'properties': {
                DATA: self.value_type.array_schema(types),
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: self.value_type.type_schema(types),
                OFFSET: types.binary()
            }
        }

    def validate_data(self, doc):
        offsets = lz4.block.decompress(doc[OFFSET])
        if len(offsets) % 4 != 0:
            raise ValueError(
                'Offsets buffer is not a multiple of byte width 4')

        counts = numpy.ndarray(len(offsets) // 4, numpy.int32, offsets)
        if not all(counts >= 0):
            raise ValueError(
                f'{self.name} offsets differences are not all non-negative')

        total = numpy.sum(counts)
        length = self.value_type.validate_data(doc[DATA])

        if length != total:
            raise ValueError(
                f'{self.name} length of values {len(values)} is different from the last offset {total}'
            )

        return len(counts) - 1


class Struct(Schema):
    name = 'struct'

    def __init__(self, fields):
        self.fields = fields

    def array_schema(self, types=BSONTypes):
        fields = {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {}
        }
        for k, v in self.fields:
            fields['required'].append(k)
            fields['properties'][k] = v.array_schema(types)

        param = []
        for k, v in self.fields:
            p = v.type_schema(types)
            p['required'].append(NAME)
            p['properties'][NAME] = types.const(k)
            param.append(p)

        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, PARAM],
            'additionalProperties': False,
            'properties': {
                DATA: {
                    'type': 'object',
                    'required': [LENGTH, FIELDS],
                    'additionalProperties': False,
                    'properties': {
                        LENGTH: types.int64(),
                        FIELDS: fields
                    }
                },
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: {
                    'type': 'array',
                    'items': param,
                    'minItems': len(param),
                    'maxItems': len(param)
                },
                OFFSET: types.binary()
            }
        }

    def validate_data(self, doc):
        length = int(doc[DATA][LENGTH])
        for k, v in self.fields:
            n = v.validate_data(doc[DATA][FIELDS][k])
            if n != length:
                raise (
                    f'{self.name} field {k} length {n} differs from array length {length}'
                )

        return length


def table_schema(fields, types=BSONTypes):
    ret = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {}
    }

    for k, v in fields:
        ret['required'].append(k)
        ret['properties'][k] = v.array_schema(types)

    return ret
