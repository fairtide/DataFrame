DataFrame Serialization with BSON Format
========================================

The library specify and implement (in C++ and Python) a BSON based
serialization for DataFrame data. The format is inspired by the data
format of `Arctic <https://github.com/manahl/arctic%3E>`__, in
particular its TickStore. However, the format covers considerably more
data types, inspired by the `Arrow <https://arrow.apache.org>`__
project.

The the specification of has some similarity to that of Arrow, it is
independent and has some notable differences.

The Python package ``bson_dataframe`` provides integration with
`pyarrow <https://arrow.apache.org/docs/python/>`__ and
`pandas <https://pandas.pydata.org>`__.

Below we detail the specification of the format, with code example of
how to encode and decode different data types.

.. code:: ipython3

    import numpy
    import lz4.block
    import bson
    import bson.json_util

.. code:: ipython3

    def test(data, dtype, encoder, decoder):
        numpy.random.seed(1204)
        mask = numpy.random.bytes(len(data) // 8 + 1)
        mask = numpy.ndarray(len(mask), numpy.uint8, mask)
        mask = numpy.unpackbits(mask)[:len(data)]
        mask = mask.astype(bool)
    
        doc = encoder(data, mask, dtype)
        d, m = decoder(doc)
        
        
        json_mode = bson.json_util.JSONMode.CANONICAL
        json_options = bson.json_util.JSONOptions(json_mode=json_mode)
        json_doc = bson.json_util.dumps(doc, json_options=json_options, indent=4)
    
        
        print(f'''
    Orignal Data
    
    data: {data}
    mask: {mask}
    type: {dtype}
    
    Encoded Document
    
    {json_doc}
    
    Decoded Data
    
    data: {d}
    mask: {m}
    ''')

Schema of BSON Keys
-------------------

.. code:: ipython3

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

Compression
-----------

.. code:: ipython3

    def compress(data) -> bson.Binary:
        if isinstance(data, numpy.ndarray):
            data = data.tobytes()
    
        return bson.Binary(lz4.block.compress(data))

.. code:: ipython3

    def decompress(data, dtype: str):
        buf = lz4.block.decompress(data)
    
        if dtype == 'raw':
            return buf
    
        wid = numpy.dtype(dtype).itemsize
        assert len(buf) % wid == 0
    
        return numpy.ndarray(len(buf) // wid, dtype, buf)

.. code:: ipython3

    def extract_type(doc):
        ret = {TYPE: doc[TYPE]}
        if PARAM in ret:
            ret[PARAM] = doc[PARAM]
    
        return ret

Mask
----

.. code:: ipython3

    def encode_mask(data: numpy.ndarray) -> bson.Binary:
        return compress(numpy.packbits(data, bitorder='big'))

.. code:: ipython3

    def decode_mask(data: bson.Binary, length) -> numpy.ndarray:
        return numpy.unpackbits(decompress(data, numpy.uint8), bitorder='big').astype(bool)[:length]

Null Array
~~~~~~~~~~

.. code:: ipython3

    def encode_null(data, *args):
        mask = numpy.zeros(len(data), dtype=bool)
    
        return {
            DATA: bson.Int64(len(data)),
            MASK: encode_mask(mask),
            TYPE: 'null',
        }

.. code:: ipython3

    def decode_null(doc):
        length = doc[DATA]
        data = [None] * length
        mask = numpy.zeros(length, dtype=bool)
    
        return data, mask

.. code:: ipython3

    test([None] * 3, 'null', encode_null, decode_null)


.. parsed-literal::

    
    Orignal Data
    
    data: [None, None, None]
    mask: [ True False False]
    type: null
    
    Encoded Document
    
    {
        "d": {
            "$numberLong": "3"
        },
        "m": {
            "$binary": {
                "base64": "AQAAABAA",
                "subType": "00"
            }
        },
        "t": "null"
    }
    
    Decoded Data
    
    data: [None, None, None]
    mask: [False False False]
    


Boolean Arrays
~~~~~~~~~~~~~~

.. code:: ipython3

    def encode_bool(data, mask, *args):
        data = numpy.array(data, dtype=bool).astype(numpy.int8)
    
        return {
            DATA: compress(data),
            MASK: encode_mask(mask),
            TYPE: 'bool',
        }

.. code:: ipython3

    def decode_bool(doc):
        data = decompress(doc[DATA], numpy.int8).astype(bool)
        mask = decode_mask(doc[MASK], len(data))
    
        return data, mask

.. code:: ipython3

    test([True, False, True], 'bool', encode_bool, decode_bool)


.. parsed-literal::

    
    Orignal Data
    
    data: [True, False, True]
    mask: [ True False False]
    type: bool
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "AwAAADABAAE=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "bool"
    }
    
    Decoded Data
    
    data: [ True False  True]
    mask: [ True False False]
    


Numeric Arrays
~~~~~~~~~~~~~~

.. code:: ipython3

    NUMERIC_TYPES = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float16', 'float32', 'float64']

.. code:: ipython3

    def encode_numeric(data, mask, dtype):
        data = numpy.array(data, dtype=dtype)
    
        return {
            DATA: compress(data),
            MASK: encode_mask(mask),
            TYPE: dtype,
        }

.. code:: ipython3

    def decode_numeric(doc):
        data = decompress(doc[DATA], doc[TYPE])
        mask = decode_mask(doc[MASK], len(data))
    
        return data, mask

.. code:: ipython3

    test([1, 2, 3], 'int32', encode_numeric, decode_numeric)


.. parsed-literal::

    
    Orignal Data
    
    data: [1, 2, 3]
    mask: [ True False False]
    type: int32
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "DAAAAMABAAAAAgAAAAMAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "int32"
    }
    
    Decoded Data
    
    data: [1 2 3]
    mask: [ True False False]
    


Date Array
----------

.. code:: ipython3

    DATE_TYPES = {'date[d]': 'int32', 'date[ms]': 'int64'}
    DATE_UNITS = {'date[d]': 'datetime64[D]', 'date[ms]': 'datetime64[ms]'}

.. code:: ipython3

    def encode_date(data, mask, dtype):
        data = numpy.array(data, dtype=DATE_TYPES[dtype])
        values = numpy.diff(data, prepend=0)
        
        return {
            DATA: compress(values.astype(DATE_TYPES[dtype])),
            MASK: encode_mask(mask),
            TYPE: dtype,
        }

.. code:: ipython3

    def decode_date(doc):
        dtype = DATE_TYPES[doc[TYPE]]
        values = numpy.cumsum(decompress(doc[DATA], dtype)).astype(DATE_UNITS[doc[TYPE]])
        mask = decode_mask(doc[MASK], len(values))
    
        return values, mask

.. code:: ipython3

    test(numpy.array(['1970-01-01', '2000-01-01'], dtype='datetime64[D]'), 'date[d]', encode_date, decode_date)


.. parsed-literal::

    
    Orignal Data
    
    data: ['1970-01-01' '2000-01-01']
    mask: [ True False]
    type: date[d]
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "CAAAAIAAAAAAzSoAAA==",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "date[d]"
    }
    
    Decoded Data
    
    data: ['1970-01-01' '2000-01-01']
    mask: [ True False]
    


.. code:: ipython3

    test(numpy.array(['1970-01-01', '2000-01-01T01:02:03.04'], dtype='datetime64[ms]'), 'date[ms]', encode_date, decode_date)


.. parsed-literal::

    
    Orignal Data
    
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [ True False]
    type: date[ms]
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "EAAAABMAAQCAIHsIa9wAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "date[ms]"
    }
    
    Decoded Data
    
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [ True False]
    


Timestamp Array
---------------

.. code:: ipython3

    TIMESTAMP_TYPES = ['timestamp[s]', 'timestamp[ms]', 'timestamp[us]', 'timestamp[ns]']

.. code:: ipython3

    def encode_timestamp(data, mask, dtype):
        data = numpy.array(data, dtype='int64')
        values = numpy.diff(data, prepend=0)
        
        return {
            DATA: compress(values.astype('int64')),
            MASK: encode_mask(mask),
            TYPE: dtype,
        }

.. code:: ipython3

    def decode_timestamp(doc):
        values = numpy.cumsum(decompress(doc[DATA], 'int64')).astype(doc[TYPE].replace('timestamp', 'datetime64'))
        mask = decode_mask(doc[MASK], len(values))
    
        return values, mask

.. code:: ipython3

    test(numpy.array(['1970-01-01', '2000-01-01T01:02:03.04'], dtype='datetime64[ms]'), 'timestamp[ms]', encode_timestamp, decode_timestamp)


.. parsed-literal::

    
    Orignal Data
    
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [ True False]
    type: timestamp[ms]
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "EAAAABMAAQCAIHsIa9wAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "timestamp[ms]"
    }
    
    Decoded Data
    
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [ True False]
    


Time Array
----------

.. code:: ipython3

    TIME_TYPES = {'time[s]': 'int32', 'time[ms]': 'int32', 'time[us]': 'int64', 'time[ns]': 'int64'}

.. code:: ipython3

    def encode_time(data, mask, dtype):
        data = numpy.array(data, dtype=TIME_TYPES[dtype])
    
        return {
            DATA: compress(data),
            MASK: encode_mask(mask),
            TYPE: dtype,
        }

.. code:: ipython3

    def decode_time(doc):
        dtype = TIME_TYPES[doc[TYPE]]
        data = decompress(doc[DATA], dtype).astype(doc[TYPE].replace('time', 'timedelta64'))
        mask = decode_mask(doc[MASK], len(data))
    
        return data, mask

.. code:: ipython3

    test(numpy.array([1, 2, 3], dtype='timedelta64[ms]'), 'time[ms]', encode_time, decode_time)


.. parsed-literal::

    
    Orignal Data
    
    data: [1 2 3]
    mask: [ True False False]
    type: time[ms]
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "DAAAAMABAAAAAgAAAAMAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "time[ms]"
    }
    
    Decoded Data
    
    data: [1 2 3]
    mask: [ True False False]
    


.. code:: ipython3

    test(numpy.array([1, 2, 3], dtype='timedelta64[ns]'), 'time[ns]', encode_time, decode_time)


.. parsed-literal::

    
    Orignal Data
    
    data: [1 2 3]
    mask: [ True False False]
    type: time[ns]
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "GAAAACIBAAEAEgIHAJAAAwAAAAAAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "time[ns]"
    }
    
    Decoded Data
    
    data: [1 2 3]
    mask: [ True False False]
    


Binary Arrays
-------------

Opaque
~~~~~~

.. code:: ipython3

    def encode_opaque(data, mask, *args):
        return {
            DATA: compress(b''.join(data)),
            MASK: encode_mask(mask),
            TYPE: 'opaque',
            PARAM: len(data[0]),
        }

.. code:: ipython3

    def decode_opaque(doc):
        data = decompress(doc[DATA], f'|S{doc[PARAM]}')
        mask = decode_mask(doc[MASK], len(data))
        
        return data, mask

.. code:: ipython3

    test([b'abc', b'def', b'ghi'], 'opaque', encode_opaque, decode_opaque)


.. parsed-literal::

    
    Orignal Data
    
    data: [b'abc', b'def', b'ghi']
    mask: [ True False False]
    type: opaque
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "CQAAAJBhYmNkZWZnaGk=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "opaque",
        "p": {
            "$numberInt": "3"
        }
    }
    
    Decoded Data
    
    data: [b'abc' b'def' b'ghi']
    mask: [ True False False]
    


Bytes and String
~~~~~~~~~~~~~~~~

.. code:: ipython3

    def encode_binary(data, mask, dtype):
        if dtype == 'utf8':
            data = [v.encode('utf8') for v in data]
    
        values = b''.join(data)
        counts = [0] + [len(v) for v in data]
        
        return {
            DATA: compress(values),
            MASK: encode_mask(mask),
            TYPE: dtype,
            OFFSET: compress(numpy.array(counts, dtype=numpy.int32)),
        }

.. code:: ipython3

    def decode_binary(doc):
        values = decompress(doc[DATA], 'raw')
        counts = decompress(doc[OFFSET], numpy.int32)
        offsets = numpy.cumsum(counts)
        n = len(offsets) - 1
    
        data = [values[offsets[i]:offsets[i + 1]] for i in range(n)]
    
        if doc[TYPE] == 'utf8':
            data = [v.decode('utf8') for v in data]
    
        mask = decode_mask(doc[MASK], len(data))
        
        return data, mask

.. code:: ipython3

    test([b'abc', b'defgh', b'ijk'], 'bytes', encode_binary, decode_binary)


.. parsed-literal::

    
    Orignal Data
    
    data: [b'abc', b'defgh', b'ijk']
    mask: [ True False False]
    type: bytes
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "CwAAALBhYmNkZWZnaGlqaw==",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "bytes",
        "o": {
            "$binary": {
                "base64": "EAAAAPABAAAAAAMAAAAFAAAAAwAAAA==",
                "subType": "00"
            }
        }
    }
    
    Decoded Data
    
    data: [b'abc', b'defgh', b'ijk']
    mask: [ True False False]
    


.. code:: ipython3

    test(['abc', 'Ωåß√'], 'utf8', encode_binary, decode_binary)


.. parsed-literal::

    
    Orignal Data
    
    data: ['abc', 'Ωåß√']
    mask: [ True False]
    type: utf8
    
    Encoded Document
    
    {
        "d": {
            "$binary": {
                "base64": "DAAAAMBhYmPOqcOlw5/iiJo=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "utf8",
        "o": {
            "$binary": {
                "base64": "DAAAAMAAAAAAAwAAAAkAAAA=",
                "subType": "00"
            }
        }
    }
    
    Decoded Data
    
    data: ['abc', 'Ωåß√']
    mask: [ True False]
    


Nested Arrays
-------------

.. code:: ipython3

    class ListType():
        def __init__(self, dtype):
            self.value_type = dtype
            
        def __str__(self):
            return f'list<{str(self.value_type)}>'

.. code:: ipython3

    class Field():
        def __init__(self, name, dtype):
            self.name = name
            self.type = dtype
            
        def __str__(self):
            return f'{self.name}: {str(self.type)}'

.. code:: ipython3

    class StructType():
        def __init__(self, fields):
            self.fields = fields
            
        def __str__(self):
            return str([str(v) for v in self.fields])

.. code:: ipython3

    class DictionaryType():
        def __init__(self, index_type, value_type, ordered):
            self.index_type = index_type
            self.value_type = value_type
            self.ordered = orderded
            
        def __str__(self):
            if self.ordered:
                return f'ordered<{str(self.index_type)}, {str(self.value_type)}>'
            else:
                return f'factor<{str(self.index_type)}, {str(self.value_type)}>'

.. code:: ipython3

    def encode_array(data, mask, dtype):
        if dtype == 'null':
            return encode_null(data, mask, dtype)
        if dtype == 'bool':
            return encode_bool(data, mask, dtype)
        if dtype in NUMERIC_TYPES:
            return encode_numeric(data, mask, dtype)
        if dtype in DATE_TYPES:
            return encode_date(data, mask, dtype)
        if dtype in TIMESTAMP_TYPES:
            return encode_timestamp(data, mask, dtype)
        if dtype in TIME_TYEPS:
            return encode_time(data, mask, dtype)
        if isinstance(dtype, ListType):
            return encode_list(data, mask, dtype)
        if isinstance(dtype, StructType):
            return encode_struct(data, mask, dtype)
        if isinstance(dtype, DictionaryType):
            return encode_dictionary(data, mask, dtype)
        raise ValueError(f'Unknown type {dtype}')

.. code:: ipython3

    def decode_array(doc):
        dtype = doc[TYPE]
        if dtype == 'null':
            return decode_null(doc)
        if dtype == 'bool':
            return decode_bool(doc)
        if dtype in NUMERIC_TYPES:
            return decode_numeric(doc)
        if dtype in DATE_TYPES:
            return decode_date(doc)
        if dtype in TIMESTAMP_TYPES:
            return decode_timestamp(doc)
        if dtype in TIME_TYEPS:
            return decode_time(doc)
        if isinstance(dtype, ListType):
            return decode_list(doc)
        if isinstance(dtype, StructType):
            return decode_struct(ddoc)
        if isinstance(dtype, DictionaryType):
            return decode_dictionary(doc)
        raise ValueError(f'Unknown type {dtype}')

List
~~~~

.. code:: ipython3

    def encode_list(data, mask, dtype):
        values = numpy.concatenate(data)
        counts = [0] + [len(v) for v in data]
        vmask = numpy.ones(len(data), dtype=bool)
        data_doc = encode_array(values, vmask, dtype.value_type)
        
        return {
            DATA: data_doc,
            MASK: encode_mask(mask),
            TYPE: 'list',
            PARAM: extract_type(data_doc),
            OFFSET: compress(numpy.array(counts, dtype=numpy.int32)),
        }

.. code:: ipython3

    def decode_list(doc):
        values, vmask = decode_array(doc[DATA])
        counts = decompress(doc[OFFSET], numpy.int32)
        offsets = numpy.cumsum(counts)
        n = len(offsets) - 1
    
        data = [values[offsets[i]:offsets[i + 1]] for i in range(n)]
        mask = decode_mask(doc[MASK], len(data))
    
        return data, mask

.. code:: ipython3

    test([numpy.array([1, 2, 3]), numpy.array([4, 5])], ListType('int64'), encode_list, decode_list)


.. parsed-literal::

    
    Orignal Data
    
    data: [array([1, 2, 3]), array([4, 5])]
    mask: [ True False]
    type: list<int64>
    
    Encoded Document
    
    {
        "d": {
            "d": {
                "$binary": {
                    "base64": "KAAAACIBAAEAEgIHACMAAwgAEwQIAIAFAAAAAAAAAA==",
                    "subType": "00"
                }
            },
            "m": {
                "$binary": {
                    "base64": "AQAAABDA",
                    "subType": "00"
                }
            },
            "t": "int64"
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "list",
        "p": {
            "t": "int64"
        },
        "o": {
            "$binary": {
                "base64": "DAAAAMAAAAAAAwAAAAIAAAA=",
                "subType": "00"
            }
        }
    }
    
    Decoded Data
    
    data: [array([1, 2, 3]), array([4, 5])]
    mask: [ True False]
    


Struct
~~~~~~

.. code:: ipython3

    def encode_struct(data, mask, dtype):
        names = list()
        types = dict()
        values = dict()
        for field in dtype.fields:
            names.append(field.name)
            types[field.name] = field.type
            values[field.name] = [v[field.name] for v in data]
        
        fields_doc = dict({k: encode_array(values[k], mask, types[k]) for k in names})
        data_doc = {LENGTH: len(data), FIELDS: fields_doc}
        
        param_doc = [extract_type(fields_doc[k]) for k in names]
        for k, v in zip(names, param_doc):
            v[NAME] = k
        
        return {
            DATA: data_doc,
            MASK: encode_mask(mask),
            TYPE: 'struct',
            PARAM: param_doc,
        }

.. code:: ipython3

    def decode_struct(doc):
        values = dict({k: decode_array(v)[0] for k, v in doc[DATA][FIELDS].items()})
    
        data = [{}] * doc[DATA][LENGTH]
        print(data)
        for k, v in values.items():
            for i, u in enumerate(v):
                print((i, k, u))
                data[i][k] = u
                print(data)
        print(data)
    
        mask = decode_mask(doc[MASK], len(data))
        
        return data, mask

.. code:: ipython3

    test([{'x': 1, 'y': 2.2}, {'x': 3, 'y': 4.4}], StructType([Field('x', 'int64'), Field('y', 'float64')]), encode_struct, decode_struct)


.. parsed-literal::

    [{}, {}]
    (0, 'x', 1)
    [{'x': 1}, {'x': 1}]
    (1, 'x', 3)
    [{'x': 3}, {'x': 3}]
    (0, 'y', 2.2)
    [{'x': 3, 'y': 2.2}, {'x': 3, 'y': 2.2}]
    (1, 'y', 4.4)
    [{'x': 3, 'y': 4.4}, {'x': 3, 'y': 4.4}]
    [{'x': 3, 'y': 4.4}, {'x': 3, 'y': 4.4}]
    
    Orignal Data
    
    data: [{'x': 1, 'y': 2.2}, {'x': 3, 'y': 4.4}]
    mask: [ True False]
    type: ['x: int64', 'y: float64']
    
    Encoded Document
    
    {
        "d": {
            "l": {
                "$numberInt": "2"
            },
            "f": {
                "x": {
                    "d": {
                        "$binary": {
                            "base64": "EAAAACIBAAEAgAMAAAAAAAAA",
                            "subType": "00"
                        }
                    },
                    "m": {
                        "$binary": {
                            "base64": "AQAAABCA",
                            "subType": "00"
                        }
                    },
                    "t": "int64"
                },
                "y": {
                    "d": {
                        "$binary": {
                            "base64": "EAAAAPABmpmZmZmZAUCamZmZmZkRQA==",
                            "subType": "00"
                        }
                    },
                    "m": {
                        "$binary": {
                            "base64": "AQAAABCA",
                            "subType": "00"
                        }
                    },
                    "t": "float64"
                }
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCA",
                "subType": "00"
            }
        },
        "t": "struct",
        "p": [
            {
                "t": "int64",
                "n": "x"
            },
            {
                "t": "float64",
                "n": "y"
            }
        ]
    }
    
    Decoded Data
    
    data: [{'x': 3, 'y': 4.4}, {'x': 3, 'y': 4.4}]
    mask: [ True False]
    


Dictionary
~~~~~~~~~~

