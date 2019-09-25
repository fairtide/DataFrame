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

import abc
import bson.json_util
import bson.raw_bson
import json
import jsonschema

DATA = 'd'
MASK = 'm'
TYPE = 't'
PARAM = 'p'

# binary/string/list
COUNT = 'o'

# list
LENGTH = 'l'

# struct
NAME = 'n'
FIELDS = 'f'

# dictionary
INDEX = 'i'
VALUE = 'd'


class _BSONTypes():
    @staticmethod
    def const(value):
        return {'enum': [value]}

    @staticmethod
    def int32():
        return {'bsonType': 'int'}

    @staticmethod
    def int64():
        return {'bsonType': 'long'}

    @staticmethod
    def binary():
        return {'bsonType': 'binData'}


class _JSONTypes():
    @staticmethod
    def const(value):
        return {'enum': [value]}

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


class Schema(abc.ABC):
    @abc.abstractmethod
    def accept(self, visitor):
        pass

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return type(self) == type(other)

    def bson_schema(self):
        return self._array_schema(_BSONTypes)

    def json_schema(self):
        return self._array_schema(_JSONTypes)

    def validate(self, doc):
        '''Validate a BSON document against the schema

        Args:
            doc (bytes or dictionary type): BSON document or dictionary to be
                encoded as BSON to be validated
        '''

        if isinstance(doc, bytes):
            doc = bson.raw_bson.RawBSONDocument(doc)

        json_mode = bson.json_util.JSONMode.CANONICAL
        json_options = bson.json_util.JSONOptions(json_mode=json_mode)
        json_doc = bson.json_util.dumps(doc, json_options=json_options)
        instance = json.loads(json_doc)
        schema = self.json_schema()
        jsonschema.validate(instance=instance, schema=schema)

    def encode(self):
        return {TYPE: self.name}

    @staticmethod
    def decode(doc):
        '''Decode the schema from BSON document

        Args:
            doc (bytes or dictionary type): BSON document or dictionary to be
                encoded as BSON to be validated

        Returns:
            A ``Schema`` object with one of its derived class
        '''

        if isinstance(doc, bytes):
            doc = bson.raw_bson.RawBSONDocument(doc)

        t = doc[TYPE]

        if PARAM in doc:
            tp = doc[PARAM]
        else:
            tp = None

        if t == 'null':
            return Null()

        if t == 'bool':
            return Bool()

        if t == 'int8':
            return Int8()

        if t == 'int16':
            return Int16()

        if t == 'int32':
            return Int32()

        if t == 'int64':
            return Int64()

        if t == 'uint8':
            return UInt8()

        if t == 'uint16':
            return UInt16()

        if t == 'uint32':
            return UInt32()

        if t == 'uint64':
            return UInt64()

        if t == 'float16':
            return Float16()

        if t == 'float32':
            return Float32()

        if t == 'float64':
            return Float64()

        if t == 'date[d]':
            return Date('d')

        if t == 'date[ms]':
            return Date('ms')

        if t == 'timestamp[s]':
            return Timestamp('s', tp)

        if t == 'timestamp[ms]':
            return Timestamp('ms', tp)

        if t == 'timestamp[us]':
            return Timestamp('us', tp)

        if t == 'timestamp[ns]':
            return Timestamp('ns', tp)

        if t == 'time[s]':
            return Time('s')

        if t == 'time[ms]':
            return Time('ms')

        if t == 'time[us]':
            return Time('us')

        if t == 'time[ns]':
            return Time('ns')

        if t == 'utf8':
            return Utf8()

        if t == 'bytes':
            return Bytes()

        if t == 'factor' or t == 'ordered':
            cls = Factor if t == 'factor' else Ordered
            if tp is None:
                index = Int32()
                value = Utf8()
            else:
                index = Schema.decode(tp[INDEX])
                value = Schema.decode(tp[VALUE])
            return cls(index, value)

        if t == 'opaque':
            return Opaque(tp)

        if t == 'list':
            return List(Schema.decode(tp))

        if t == 'struct':
            return Struct([(f[NAME], Schema.decode(f)) for f in tp])

        raise ValueError(f'{t} is not supported BSON DataFrame type')

    def _array_schema(self, types):
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

    def _type_schema(self, types):
        sch = self._array_schema(types)

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


class Null(Schema):
    name = 'null'

    def accept(self, visitor):
        return visitor.visit_null(self)

    def _array_schema(self, types):
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


class Bool(Schema):
    name = 'bool'
    byte_width = 1

    def accept(self, visitor):
        return visitor.visit_bool(self)


class Int8(Schema):
    name = 'int8'
    byte_width = 1

    def accept(self, visitor):
        return visitor.visit_int8(self)


class Int16(Schema):
    name = 'int16'
    byte_width = 2

    def accept(self, visitor):
        return visitor.visit_int16(self)


class Int32(Schema):
    name = 'int32'
    byte_width = 4

    def accept(self, visitor):
        return visitor.visit_int32(self)


class Int64(Schema):
    name = 'int64'
    byte_width = 8

    def accept(self, visitor):
        return visitor.visit_int64(self)


class UInt8(Schema):
    name = 'uint8'
    byte_width = 1

    def accept(self, visitor):
        return visitor.visit_uint8(self)


class UInt16(Schema):
    name = 'uint16'
    byte_width = 2

    def accept(self, visitor):
        return visitor.visit_uint16(self)


class UInt32(Schema):
    name = 'uint32'
    byte_width = 4

    def accept(self, visitor):
        return visitor.visit_uint32(self)


class UInt64(Schema):
    name = 'uint64'
    byte_width = 8

    def accept(self, visitor):
        return visitor.visit_uint64(self)


class Float16(Schema):
    name = 'float16'
    byte_width = 2

    def accept(self, visitor):
        return visitor.visit_float16(self)


class Float32(Schema):
    name = 'float32'
    byte_width = 4

    def accept(self, visitor):
        return visitor.visit_float32(self)


class Float64(Schema):
    name = 'float64'
    byte_width = 8

    def accept(self, visitor):
        return visitor.visit_float64(self)


class Date(Schema):
    def __init__(self, unit):
        assert unit in ('d', 'ms')

        self._unit = unit
        self._name = f'date[{unit}]'

        if unit == 'd':
            self._byte_width = 4

        if unit == 'ms':
            self._byte_width = 8

    def accept(self, visitor):
        return visitor.visit_date(self)

    def __eq__(self, other):
        return type(self) == type(other) and self.unit == other.unit

    @property
    def unit(self):
        return self._unit

    @property
    def name(self):
        return self._name

    @property
    def byte_width(self):
        return self._byte_width


class Timestamp(Schema):
    byte_width = 8

    def __init__(self, unit, tz=None):
        assert unit in ('s', 'ms', 'us', 'ns')

        self._unit = unit
        self._name = f'timestamp[{unit}]'
        self._tz = tz

    def accept(self, visitor):
        return visitor.visit_timestamp(self)

    def __str__(self):
        if self.tz is None:
            return self.name
        return self.name + ' ' + self.tz

    def __eq__(self, other):
        return type(self) == type(other) and \
                self.unit == other.unit and self.tz == other.tz

    @property
    def unit(self):
        return self._unit

    @property
    def name(self):
        return self._name

    @property
    def tz(self):
        return self._tz

    def encode(self):
        ret = {TYPE: self.name}
        if isinstance(self.tz, str):
            ret[PARAM] = self.tz

        return ret

    def _array_schema(self, types):
        ret = super()._array_schema(types)
        ret['properties'][PARAM] = {'type': 'string'}

        return ret


class Time(Schema):
    def __init__(self, unit):
        assert unit in ('s', 'ms', 'us', 'ns')

        self._unit = unit
        self._name = f'time[{unit}]'

        if unit == 's' or unit == 'ms':
            self._byte_width = 4

        if unit == 'us' or unit == 'ns':
            self._byte_width = 8

    def accept(self, visitor):
        return visitor.visit_time(self)

    def __eq__(self, other):
        return type(self) == type(other) and self.unit == other.unit

    @property
    def unit(self):
        return self._unit

    @property
    def name(self):
        return self._name

    @property
    def byte_width(self):
        return self._byte_width


class Opaque(Schema):
    name = 'opaque'

    def __init__(self, byte_width):
        self._byte_width = int(byte_width)

    def accept(self, visitor):
        return visitor.visit_opaque(self)

    def __str__(self):
        return f'{self.name}[{self.byte_width}]'

    def __eq__(self, other):
        return type(self) == type(
            other) and self.byte_width == other.byte_width

    @property
    def byte_width(self):
        return self._byte_width

    def encode(self):
        return {TYPE: self.name, PARAM: self.byte_width}

    def _array_schema(self, types):
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


class Binary(Schema):
    def _array_schema(self, types):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, COUNT],
            'additionalProperties': False,
            'properties': {
                DATA: types.binary(),
                MASK: types.binary(),
                TYPE: types.const(self.name),
                COUNT: types.binary()
            }
        }


class Bytes(Binary):
    name = 'bytes'

    def accept(self, visitor):
        return visitor.visit_bytes(self)


class Utf8(Binary):
    name = 'utf8'

    def accept(self, visitor):
        return visitor.visit_utf8(self)


class Dictionary(Schema):
    def __init__(self, index, value):
        assert isinstance(index, Schema)
        assert isinstance(value, Schema)
        assert type(index) in (Int8, Int16, Int32, Int64)

        self._index = index
        self._value = value

    def __str__(self):
        return f'{self.name}[{str(self.index)}, {str(self.value)}]'

    def __eq__(self, other):
        return type(self) == type(
            other) and self.index == other.index and self.value == other.value

    @property
    def index(self):
        return self._index

    @property
    def value(self):
        return self._value

    def encode(self):
        return {
            TYPE: self.name,
            PARAM: {
                INDEX: self.index.encode(),
                VALUE: self.value.encode()
            }
        }

    def _array_schema(self, types):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE],
            'additionalProperties': False,
            'properties': {
                DATA: {
                    'type': 'object',
                    'required': [INDEX, VALUE],
                    'additionalProperties': False,
                    'properties': {
                        INDEX: self.index._array_schema(types),
                        VALUE: self.value._array_schema(types)
                    }
                },
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: {
                    'type': 'object',
                    'required': [INDEX, VALUE],
                    'additionalProperties': False,
                    'properties': {
                        INDEX: self.index._type_schema(types),
                        VALUE: self.value._type_schema(types)
                    }
                }
            }
        }


class Ordered(Dictionary):
    name = 'ordered'

    def accept(self, visitor):
        return visitor.visit_ordered(self)


class Factor(Dictionary):
    name = 'factor'

    def accept(self, visitor):
        return visitor.visit_factor(self)


class List(Schema):
    name = 'list'

    def __init__(self, value):
        assert isinstance(value, Schema)

        self._value = value

    def accept(self, visitor):
        return visitor.visit_list(self)

    def __str__(self):
        return f'{self.name}[{str(self.value)}]'

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    @property
    def value(self):
        return self._value

    def encode(self):
        return {TYPE: self.name, PARAM: self.value.encode()}

    def _array_schema(self, types):
        return {
            'type': 'object',
            'required': [DATA, MASK, TYPE, PARAM, COUNT],
            'additionalProperties': False,
            'properties': {
                DATA: self.value._array_schema(types),
                MASK: types.binary(),
                TYPE: types.const(self.name),
                PARAM: self.value._type_schema(types),
                COUNT: types.binary()
            }
        }


class Struct(Schema):
    name = 'struct'

    def __init__(self, fields):
        self._fields = list()
        for k, v in fields:
            assert isinstance(k, str)
            assert isinstance(v, Schema)
            self._fields.append((k, v))

    def accept(self, visitor):
        return visitor.visit_struct(self)

    def __str__(self):
        fields = ', '.join([k + ': ' + str(v) for k, v in self.fields])
        return f'{self.name}[{fields}]'

    def __eq__(self, other):
        return type(self) == type(other) and self.fields == other.fields

    @property
    def fields(self):
        return self._fields

    def encode(self):
        param = list()
        for k, v in self.fields:
            p = {NAME: k}
            p.update(v.encode())
            param.append(p)

        return {TYPE: self.name, PARAM: param}

    def _array_schema(self, types):
        fields = {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {}
        }
        for k, v in self.fields:
            fields['required'].append(k)
            fields['properties'][k] = v._array_schema(types)

        param = []
        for k, v in self.fields:
            p = v._type_schema(types)
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
                COUNT: types.binary()
            }
        }
