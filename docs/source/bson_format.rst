.. code:: python3

    import bson
    import bson.json_util
    import bson.raw_bson
    import lz4.block
    import numpy

DataFrame Serialization with BSON Format
========================================

A BSON based serialization for DataFrame or similar tabular data is
specified here. The format is inspired by the data format used by
`Arctic <https://github.com/manahl/arctic%3E>`__, especially its
TickStore. However, the format covers considerably more data types,
inspired by the `Arrow <https://arrow.apache.org>`__ project. Also
unlike Arctic, the format is language agnostics. Below we detail the
specification of the format, with code example of a minimal reference
implementation that integrate with `numpy <https://numpy.org>`__.

Basics
------

Byte order
~~~~~~~~~~

The format use little-endian for byte order

Schema
~~~~~~

Each array is encoded into a BSON document. There are five possible keys
at top level. The exact contents are details later. Three are them are
required

.. code:: python3

    DATA = 'd'
    MASK = 'm'
    TYPE = 't'

Two top level fields are optional,

.. code:: python3

    PARAM = 'p'
    OFFSET = 'o'

There are a few sub field which may appears within ``DATA`` or
``PARAM``.

.. code:: python3

    LENGTH = 'l'
    NAME = 'n'
    FIELDS = 'f'
    INDEX = 'i'
    DICT = 'd'

Buffers
~~~~~~~

The bulk of the data are encoded as BSON binary compressed with
`LZ4 <http://www.lz4.org>`__ `block
format <https://github.com/lz4/lz4/blob/master/doc/lz4_Block_format.md>`__
prefixed with a 32-bits integer of the orignal size.

.. code:: python3

    def compress(data: numpy.ndarray) -> bson.Binary:
        return bson.Binary(lz4.block.compress(data.tobytes()))

.. code:: python3

    def decompress(data: bson.Binary, dtype='byte') -> numpy.ndarray:
        buf = lz4.block.decompress(data)
        wid = numpy.dtype(dtype).itemsize
        assert len(buf) % wid == 0
        return numpy.ndarray(len(buf) // wid, dtype, buf)

Mask
~~~~

Arrays may be masked to indicate positions of missings values. The
length of the mask the the same as the array data. When encoded, a mask
is packed bits with MSB order.

.. code:: python3

    def encode_mask(data: numpy.ndarray) -> bson.Binary:
        return compress(numpy.packbits(data))

When unpacked, the mask may have at most 7 trailing zero bits.

.. code:: python3

    def decode_mask(data: bson.Binary, length: int) -> numpy.ndarray:
        buf = decompress(data, 'uint8')
        mask = numpy.unpackbits(buf).astype(bool)
        assert not any(mask[length:])
        assert length <= len(mask) < length + 8
        return mask[:length]

Offsets
~~~~~~~

For arrays with variable length elements (``bytes``, ``utf8``,
``list``), the length of each element is encoded by their offsets within
a concatenated array of all values. The offsets data itself is
difference encoded.

Example, given data

::

   data = [[1, 2, 3], [], [4, 5], [6]]

The concatenated array is,

::

   values = [1, 2, 3, 4, 5, 6]

And the corresponding offsets is,

::

   offsets = [0, 3, 3, 5, 6]

And the difference encoded offsets is,

::

   counts = [0, 3, 0, 2, 1]

The following relation always hold (within index range):

::

   data[i] == values[offsets[i]:offsets[i + 1]]

The ``counts`` array is stored as 32-bits integers.

.. code:: python3

    def encode_offsets(data) -> bson.Binary:
        return compress(numpy.array([0] + [len(v) for v in data], 'int32'))

.. code:: python3

    def decode_offsets(data, offsets: bson.Binary):
        offsets = numpy.cumsum(decompress(offsets, 'int32'))
        return [data[i:j] for i, j in zip(offsets[:-1], offsets[1:])]

Data Types
----------

Below we list all data types supported by the format.

Types with fixed byte width elements

================= ==========
Type              Byte width
================= ==========
``null``          0
``bool``          1
``int8``          1
``int16``         2
``int32``         4
``int64``         8
``uint8``         1
``uint16``        2
``uint32``        4
``uint64``        8
``float16``       2
``float32``       4
``float64``       8
``date[d]``       4
``date[ms]``      8
``timestamp[s]``  8
``timestamp[ms]`` 8
``timestamp[us]`` 8
``timestamp[ns]`` 8
``time[s]``       4
``time[ms]``      4
``time[us]``      8
``time[ns]``      8
================= ==========

Other types may be parametric, nested or with variable length elements

=========== ========== ====== ===============
Type        Parametric Nested Variable Length
=========== ========== ====== ===============
``opaque``  True       False  False
``bytes``   False      False  True
``utf8``    False      False  True
``ordered`` True       True   False
``factor``  True       True   False
``list``    True       True   True
``struct``  True       True   False
=========== ========== ====== ===============

Below we define the base class for all data types. Different data type
will at least override the ``encode_data`` and ``decode_data`` methods
for the ``DATA`` field. For parameteric types the ``encode_param`` and
``decode_param`` fields are required for the ``PARAM`` field.

.. code:: python3

    class DataType(object):
        name = None
        has_param = False
        has_offsets = False
    
        def __str__(self):
            return self.name
    
        def to_numpy(self):
            return self.name
    
        def encode_param(self):
            raise NotImplementedError()
    
        def decode_param(self):
            raise NotImplementedError()
    
        def encode_data(self):
            raise NotImplementedError()
    
        def decode_data(self):
            raise NotImplementedError()
    
        def encode_type(self):
            doc = {}
            doc[TYPE] = self.name
    
            if self.has_param:
                doc[PARAM] = self.encode_param()
    
            return bson.raw_bson.RawBSONDocument(bson.encode(doc))
    
        @staticmethod
        def decode_type(doc):
            typename = doc[TYPE].title()
            typename = typename.replace('[', '')
            typename = typename.replace(']', '')
            dtype = globals()[typename]()
    
            if PARAM in doc:
                dtype.decode_param(doc[PARAM])
    
            return dtype
    
        def encode_array(self, data, mask):
            doc = {}
            doc[DATA] = self.encode_data(data)
            doc[MASK] = encode_mask(mask)
    
            doc.update(self.encode_type())
    
            if self.has_offsets:
                doc[OFFSET] = encode_offsets(data)
    
            return bson.raw_bson.RawBSONDocument(bson.encode(doc))
    
        def decode_array(self, doc):
            if PARAM in doc:
                self.decode_param(doc[PARAM])
    
            data = self.decode_data(doc[DATA])
    
            if OFFSET in doc:
                data = decode_offsets(data, doc[OFFSET])
    
            mask = decode_mask(doc[MASK], len(data))
        
            return data, mask

Below we define a simple test functions to display the encoded data in
`MongoDB Canonical
JSON <https://github.com/mongodb/specifications/blob/master/source/extended-json.rst>`__,

.. code:: python3

    def test(dtype, data, mask):
        print(f'''
    Orignal
    
    type: {dtype}
    data: {data}
    mask: {mask}
    ''')
    
        doc = dtype.encode_array(data, mask)
        json_mode = bson.json_util.JSONMode.CANONICAL
        json_options = bson.json_util.JSONOptions(json_mode=json_mode)
        json_doc = bson.json_util.dumps(doc, json_options=json_options, indent=4)
    
        print(f'''
    Encoded
    
    {json_doc}
    ''')
    
        t = DataType.decode_type(doc)
        d, m = t.decode_array(doc)
    
        print(f'''
    Decoded
    
    type: {t}
    data: {d}
    mask: {m}
    ''')

Null
~~~~

A ``null`` array is one with all values missing. The ``DATA`` is its
encoded as a 64-bit BSON integer. Its mask is always an array of
``False`` with the same length.

.. code:: python3

    class Null(DataType):
        name = 'null'
    
        def to_numpy(self):
            return object
    
        def encode_data(self, data):
            return bson.Int64(len(data))
    
        def decode_data(self, data):
            return [None] * data

.. code:: python3

    test(Null(), [None, None, None], [False, False, False])


.. parsed-literal::

    
    Orignal
    
    type: null
    data: [None, None, None]
    mask: [False, False, False]
    
    
    Encoded
    
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
    
    
    Decoded
    
    type: null
    data: [None, None, None]
    mask: [False False False]
    


Numeric
~~~~~~~

Numeric arrays are encoded as-is. The ``DATA`` is its underlying bytes.
All the standard numeric types are supported.

.. code:: python3

    def numeric_type(dtype):
        clsname = dtype.title()
    
        def encode_data(self, data):
            return compress(numpy.array(data, dtype))
    
        def decode_data(self, data):
            return decompress(data, dtype)
    
        return clsname, type(clsname, (DataType,), {
            'name': dtype,
            'encode_data': encode_data,
            'decode_data': decode_data
        })

.. code:: python3

    for dtype in ['bool', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float16', 'float32', 'float64']:
        clsname, typeclass = numeric_type(dtype)
        globals()[clsname] = typeclass

.. code:: python3

    test(Int32(), [1, 2, 3], [False, True, False])


.. parsed-literal::

    
    Orignal
    
    type: int32
    data: [1, 2, 3]
    mask: [False, True, False]
    
    
    Encoded
    
    {
        "d": {
            "$binary": {
                "base64": "DAAAAMABAAAAAgAAAAMAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABBA",
                "subType": "00"
            }
        },
        "t": "int32"
    }
    
    
    Decoded
    
    type: int32
    data: [1 2 3]
    mask: [False  True False]
    


Date
~~~~

Date array is similar to numeric types in that they also have flat
memory layout. The values are the days or milliseconds from UNIX epoch.
They differ in that they are difference encoded before compressed. This
leads to better compression ratio for many often occurring data such as
financial data. For random data there shall be no difference in
compression ratio on average and the code of encoding and decoding the
difference shall be minimal compared to that of memory allocation etc.

For example, consider the following sequence of integers and the
compressed size of the orignal and difference encoded data,

.. code:: python3

    data = numpy.array(range(1000), 'int32')
    len(compress(data)), len(compress(numpy.diff(data, prepend=numpy.int32(0))))




.. parsed-literal::

    (4013, 34)



The later is more than 100 times smaller than compressing the original
values. Here’s is another example of random integers,

.. code:: python3

    numpy.random.seed(0)
    data = numpy.random.randint(-1000, 1000, 1000, 'int32')
    len(compress(data)), len(compress(numpy.diff(data, prepend=numpy.int32(0))))




.. parsed-literal::

    (3829, 3868)



The compressed size of the later is comaprable to that of the original.
Date array can have two different unit and underlying integer type. The
underlying integer type is 32 bits sigend integer for day unit and 64
bits integer for milliseconds unit.

.. code:: python3

    class Date(DataType):
        @property
        def name(self):
            return f'date[{self.unit}]'
    
        @property
        def dtype(self):
            return f'int{self.bit_width}'
    
        def encode_data(self, data):
            data = numpy.array(data, self.dtype)
            values = numpy.diff(data, prepend=0).astype(self.dtype)
            return compress(values)
    
        def decode_data(self, data):
            values = decompress(data, self.dtype)
            return numpy.cumsum(values).astype(self.to_numpy())

.. code:: python3

    class DateD(Date):
        unit = 'd'
        bit_width = 32
    
        def to_numpy(self):
            return 'datetime64[D]'

.. code:: python3

    class DateMs(Date):
        unit = 'ms'
        bit_width = 32
    
        def to_numpy(self):
            return 'datetime64[ms]'

.. code:: python3

    test(DateD(), numpy.array(['1970-01-01', '2000-01-01'], 'datetime64[D]'), [True, False])


.. parsed-literal::

    
    Orignal
    
    type: date[d]
    data: ['1970-01-01' '2000-01-01']
    mask: [True, False]
    
    
    Encoded
    
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
    
    
    Decoded
    
    type: date[d]
    data: ['1970-01-01' '2000-01-01']
    mask: [ True False]
    


.. code:: python3

    test(DateMs(), numpy.array(['1970-01-01', '2000-01-01T01:02:03.04'], 'datetime64[ms]'), [True, False])


.. parsed-literal::

    
    Orignal
    
    type: date[ms]
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [True, False]
    
    
    Encoded
    
    {
        "d": {
            "$binary": {
                "base64": "CAAAAIAAAAAAIHsIaw==",
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
    
    
    Decoded
    
    type: date[ms]
    data: ['1970-01-01T00:00:00.000' '1970-01-21T18:48:37.920']
    mask: [ True False]
    


Timestamp
~~~~~~~~~

Timestamp array is encoded the same way as the date array, with the
following difference,

-  The underlying numeric type is aways 64 bits integers
-  The unit may be one of second, millisecond, microsecond and
   nanosecond
-  The data type may have an optional string parameter to indicate its
   timezone

It is equivalent to ``numpy.datetime64`` type

.. code:: python3

    class Timestamp(DataType):
        @property
        def name(self):
            return f'timestamp[{self.unit}]'
     
        def to_numpy(self):
            return f'datetime64[{self.unit}]'
    
        def encode_data(self, data):
            dtype = 'int64'
            data = numpy.array(data, dtype)
            values = numpy.diff(data, prepend=0).astype(dtype)
    
            return compress(values)
    
        def decode_data(self, data):
            dtype = 'int64'
            values = decompress(data, dtype)
    
            return numpy.cumsum(values).astype(self.to_numpy())

.. code:: python3

    class TimestampS(Timestamp):
        unit = 's'

.. code:: python3

    class TimestampMs(Timestamp):
        unit = 'ms'

.. code:: python3

    class TimestampUs(Timestamp):
        unit = 'us'

.. code:: python3

    class TimestampNs(Timestamp):
        unit = 'ns'

.. code:: python3

    test(TimestampMs(), numpy.array(['1970-01-01', '2000-01-01T01:02:03.04'], 'datetime64[ms]'), [True, False])


.. parsed-literal::

    
    Orignal
    
    type: timestamp[ms]
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [True, False]
    
    
    Encoded
    
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
    
    
    Decoded
    
    type: timestamp[ms]
    data: ['1970-01-01T00:00:00.000' '2000-01-01T01:02:03.040']
    mask: [ True False]
    


Time
~~~~

Time array is used to represent time of day and has the same unit
choices as timestamp array. The encoding is the same as numeric types.
The underlying integer type is 32 bits signed integers for second and
millisecond units, and 64 bits integer for microsecond and nanosecond
unit.

.. code:: python3

    class Time(DataType):
        @property
        def name(self):
            return f'time[{self.unit}]'
    
        @property
        def dtype(self):
            return f'int{self.bit_width}'
    
        def to_numpy(self):
            return f'timedelta64[{self.unit}]'
    
        def encode_data(self, data):
            return compress(numpy.array(data, self.dtype))
    
        def decode_data(self, data):
            return decompress(data, self.dtype).astype(self.to_numpy())

.. code:: python3

    class TimeS(Time):
        unit = 's'
        bit_width = 32

.. code:: python3

    class TimeMs(Time):
        unit = 'ms'
        bit_width = 32

.. code:: python3

    class TimeUs(Time):
        unit = 'us'
        bit_width = 64

.. code:: python3

    class TimeNs(Time):
        unit = 'ns'
        bit_width = 64

.. code:: python3

    test(TimeMs(), numpy.array([1, 2, 3], 'timedelta64[ms]'), [True, False, True])


.. parsed-literal::

    
    Orignal
    
    type: time[ms]
    data: [1 2 3]
    mask: [True, False, True]
    
    
    Encoded
    
    {
        "d": {
            "$binary": {
                "base64": "DAAAAMABAAAAAgAAAAMAAAA=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCg",
                "subType": "00"
            }
        },
        "t": "time[ms]"
    }
    
    
    Decoded
    
    type: time[ms]
    data: [1 2 3]
    mask: [ True False  True]
    


Opaque
~~~~~~

Opaque array has fixed length bytes as its element. It is encoded using
the concatenated bytes and the length of each element.

.. code:: python3

    class Opaque(DataType):
        name = 'opaque'
        has_param = True
    
        def __init__(self, byte_width=0):
            self.byte_width = byte_width
    
        def __str__(self):
            return f'opaque[{self.byte_width}]'
    
        def to_numpy(self):
            return f'|S{self.byte_width}'
    
        def encode_param(self):
            return self.byte_width
    
        def decode_param(self, param):
            self.byte_width = param
    
        def encode_data(self, data):
            buf = b''.join(data)
            assert len(buf) == len(data) * self.byte_width
            return compress(numpy.ndarray(len(data), self.to_numpy(), buf))
    
        def decode_data(self, data):
            return decompress(data, self.to_numpy())

.. code:: python3

    test(Opaque(3), [b'abc', b'def', b'ghi'], [True, False, True])


.. parsed-literal::

    
    Orignal
    
    type: opaque[3]
    data: [b'abc', b'def', b'ghi']
    mask: [True, False, True]
    
    
    Encoded
    
    {
        "d": {
            "$binary": {
                "base64": "CQAAAJBhYmNkZWZnaGk=",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCg",
                "subType": "00"
            }
        },
        "t": "opaque",
        "p": {
            "$numberInt": "3"
        }
    }
    
    
    Decoded
    
    type: opaque[3]
    data: [b'abc' b'def' b'ghi']
    mask: [ True False  True]
    


Bytes and String
~~~~~~~~~~~~~~~~

Byte and string arrays are identical in their encoding, with the later
distinguish from the former in that its values may be decoded as UTF-8
code points. They are encoded by two parts

-  ``DATA`` is the concatenated bytes of the values
-  ``OFFSET`` is the 32-bits integers of difference encoded offset of
   each value within the concatenated bytes. This is the same the number
   of bytes within each element

Note that for String array, the offsets are offsets into the raw bytes,
not the characters. Each UTF-8 code point may occupy more than 1 byte.

.. code:: python3

    class Bytes(DataType):
        name = 'bytes'
        has_offsets = True
    
        def to_numpy(self):
            return object
    
        def encode_data(self, data):
            buf = b''.join(data)
            return compress(numpy.ndarray(len(buf), 'byte', buf))
    
        def decode_data(self, data):
            return decompress(data).tobytes()

.. code:: python3

    class Utf8(Bytes):
        name = 'utf8'
    
        def encode_array(self, data, mask):
            return super().encode_array([v.encode('utf8') for v in data], mask)
    
        def decode_array(self, doc):
            data, mask = super().decode_array(doc)
            return [v.decode('utf8') for v in data], mask

.. code:: python3

    test(Bytes(), [b'abc', b'defgh', b'ijk'], [True, False, True])


.. parsed-literal::

    
    Orignal
    
    type: bytes
    data: [b'abc', b'defgh', b'ijk']
    mask: [True, False, True]
    
    
    Encoded
    
    {
        "d": {
            "$binary": {
                "base64": "CwAAALBhYmNkZWZnaGlqaw==",
                "subType": "00"
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCg",
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
    
    
    Decoded
    
    type: bytes
    data: [b'abc', b'defgh', b'ijk']
    mask: [ True False  True]
    


.. code:: python3

    test(Utf8(), ['abc', 'Ωåß√'], [True, False])


.. parsed-literal::

    
    Orignal
    
    type: utf8
    data: ['abc', 'Ωåß√']
    mask: [True, False]
    
    
    Encoded
    
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
    
    
    Decoded
    
    type: utf8
    data: ['abc', 'Ωåß√']
    mask: [ True False]
    


Dictionary
~~~~~~~~~~

Dictionary encoding, also called categorical arrays is a compact way to
encode data with values falls in a set of categories. For example,

Given data

::

   data = ['a', 'a', 'b', 'c', 'a', 'd', 'c']

It may be encoded by two arrays. First dictionary, the set of all
possible values,

::

   value = ['a', 'b', 'c', 'd']

And second the zero-based index within the dictionary,

::

   index = [0, 0, 1, 2, 0, 3, 2]

The following relaton holds,

::

   data[i] == value[index[i]]

Some languages such as R makes the distinction between ordered and
unordered categorical arrays. The format allows such distinction but
otherwise the encoding is identical for both:

-  ``DATA`` is a BSON document with fields

   -  ``INDEX`` encoded index array
   -  ``DICT`` encoded dictionary array

-  ``PARAM`` is optional. If ommited, the index type is ``int32`` and
   the dictionary type is ``utf8``. If present, it is a BSON document
   with fields.

   -  ``INDEX`` encoded type of the index array
   -  ``DICT`` encoded type of the dictionary array

Whether or not ``PARAM`` is explicitly given, the types given in
``PARAM`` or the defaults shall match that of the actual array types in
``DATA``.

The implementation is responsible to maintain suitable order of the
values in the dictionary if such distinction is important.

.. code:: python3

    class Dictionary(DataType):
        def __init__(self, index_type=None, value_type=None):
            if index_type is None:
                index_type = Int32()
    
            if value_type is None:
                value_type = Utf8()
    
            self.index_type = index_type
            self.value_type = value_type
            
        def __str__(self):
            return f'{self.name}[{str(self.index_type)}, {str(self.value_type)}]'
    
        def to_numpy(self):
            return object
    
        def encode_param(self):
            return {
                INDEX: self.index_type.encode_type(),
                DICT: self.value_type.encode_type(),
            }
    
        def decode_param(self, param):
            self.index_type = DataType.decode_type(param[INDEX])
            self.value_type = DataType.decode_type(param[DICT])
    
        def encode_data(self, data):
            value = numpy.unique(data).tolist()
            index = [value.index(v) for v in data]
            index_mask = numpy.ones(len(index), bool)
            value_mask = numpy.ones(len(value), bool)
    
            return {
                INDEX: self.index_type.encode_array(index, index_mask),
                DICT: self.value_type.encode_array(value, value_mask),
            }
    
        def decode_data(self, data):
            index, index_mask = self.index_type.decode_array(data[INDEX])
            value, value_mask = self.value_type.decode_array(data[DICT])
    
            return [value[i] for i in index]

.. code:: python3

    class Ordered(Dictionary):
        name = 'ordered'

.. code:: python3

    class Factor(Dictionary):
        name = 'factor'

.. code:: python3

    test(Ordered(), ['abc', 'abc', 'def', 'xyz', 'abc'], [True, True, True, False, True])


.. parsed-literal::

    
    Orignal
    
    type: ordered[int32, utf8]
    data: ['abc', 'abc', 'def', 'xyz', 'abc']
    mask: [True, True, True, False, True]
    
    
    Encoded
    
    {
        "d": {
            "i": {
                "d": {
                    "$binary": {
                        "base64": "FAAAABMAAQDAAQAAAAIAAAAAAAAA",
                        "subType": "00"
                    }
                },
                "m": {
                    "$binary": {
                        "base64": "AQAAABD4",
                        "subType": "00"
                    }
                },
                "t": "int32"
            },
            "d": {
                "d": {
                    "$binary": {
                        "base64": "CQAAAJBhYmNkZWZ4eXo=",
                        "subType": "00"
                    }
                },
                "m": {
                    "$binary": {
                        "base64": "AQAAABDg",
                        "subType": "00"
                    }
                },
                "t": "utf8",
                "o": {
                    "$binary": {
                        "base64": "EAAAAPABAAAAAAMAAAADAAAAAwAAAA==",
                        "subType": "00"
                    }
                }
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABDo",
                "subType": "00"
            }
        },
        "t": "ordered"
    }
    
    
    Decoded
    
    type: ordered[int32, utf8]
    data: ['abc', 'abc', 'def', 'xyz', 'abc']
    mask: [ True  True  True False  True]
    


List
~~~~

List array is an array with each element being a list itself, with the
same type. The data type is defined by the value type of the elements.

It is encoded similar to that of bytes and string array,

-  ``DATA`` is the encoded document of the concatenated values
-  ``OFFSET`` is the 32-bits integers of difference encoded offset of
   each value within the concatenated bytes. This is the same the number
   of bytes within each element
-  ``PARAM`` is the encoded value type. Note that, while decoding, the
   value type can be inferred from the encoded value array. And the
   inferred type shall match the type specified here.

The value array may have its own mask. The value type given in ``PARAM``
shall match that of the actual type in ``DATA``

Note that, the length of missing element may or may not be zero.

.. code:: python3

    class List(DataType):
        name = 'list'
        has_param = True
        has_offsets = True
    
        def __init__(self, value_type=None):
            self.value_type = value_type
    
        def __str__(self):
            return f'list[{str(self.value_type)}]'
    
        def to_numpy(self):
            return object
    
        def encode_param(self):
            return self.value_type.encode_type()
    
        def decode_param(self, param):
            self.value_type = DataType.decode_type(param)
    
        def encode_data(self, data):
            values = numpy.concatenate(data)
            values_mask = numpy.ones(len(values), bool) # not the general case
            return self.value_type.encode_array(values, values_mask)
    
        def decode_data(self, data):
            values, values_mask = self.value_type.decode_array(data)
            return values

.. code:: python3

    test(List(Int64()), [[1, 2, 3], [], [], [4, 5]], [True, False, True, True])


.. parsed-literal::

    
    Orignal
    
    type: list[int64]
    data: [[1, 2, 3], [], [], [4, 5]]
    mask: [True, False, True, True]
    
    
    Encoded
    
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
                    "base64": "AQAAABD4",
                    "subType": "00"
                }
            },
            "t": "int64"
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCw",
                "subType": "00"
            }
        },
        "t": "list",
        "p": {
            "t": "int64"
        },
        "o": {
            "$binary": {
                "base64": "FAAAAFAAAAAAAwUAsAAAAAAAAAACAAAA",
                "subType": "00"
            }
        }
    }
    
    
    Decoded
    
    type: list[int64]
    data: [array([1, 2, 3]), array([], dtype=int64), array([], dtype=int64), array([4, 5])]
    mask: [ True False  True  True]
    


Struct
~~~~~~

Struct array is similar to that of numpy structured array. Logically
each element is a record with given fields.

.. code:: python3

    class Field():
        def __init__(self, name, dtype):
            self.name = name
            self.type = dtype
    
        def __str__(self):
            return f'{self.name}: {str(self.type)}'
    
        def encode(self):
            doc = {NAME: self.name}
            doc.update(self.type.encode_type())
    
            return doc
    
        @staticmethod
        def decode(doc):
            return Field(doc[NAME], DataType.decode_type(doc))

.. code:: python3

    class Struct(DataType):
        name = 'struct'
        has_param = True
    
        def __init__(self, fields=None):
            self.fields = fields
    
        def __str__(self):
            return f'struct{[str(v) for v in self.fields]}'
    
        def to_numpy(self):
            return numpy.dtype([(field.name, field.type.to_numpy()) for field in self.fields])
    
        def encode_param(self):
            return [v.encode() for v in self.fields]
    
        def decode_param(self, param):
            self.fields = [Field.decode(v) for v in param]
    
        def encode_data(self, data):
            mask = numpy.ones(len(data), bool)
    
            return {
                LENGTH: bson.Int64(len(data)),
                FIELDS: dict({
                    field.name: field.type.encode_array(data[field.name], mask)
                    for field in self.fields
                }),
            }
    
        def decode_data(self, data):
            length = data[LENGTH]
    
            fields = dict({
                field.name: field.type.decode_array(data[FIELDS][field.name])
                for field in self.fields
            })
    
            data = numpy.ndarray(length, self.to_numpy())
            for k, v in fields.items():
                data[k] = v[0]
    
            return data

An example of struct array,

.. code:: python3

    struct_type = Struct([Field('x', Int64()), Field('y', Float64())])
    
    struct_data = numpy.zeros(3, struct_type.to_numpy())
    struct_data['x'] = [1, 2, 3]
    struct_data['y'] = [4, 5, 6]
    
    struct_data




.. parsed-literal::

    array([(1, 4.), (2, 5.), (3, 6.)], dtype=[('x', '<i8'), ('y', '<f8')])



The storage while encoded is column based, that is, the gathered values
of each field is encoded separatedly. More specifically

-  ``DATA`` is a document with with two field:

   -  ``LENGTH`` the length of the overall struct array
   -  ``FIELDS`` a document of with each element itself is a document,

      -  The key of the element is the field name
      -  The value of the element is the encoded document of an array of
         all values for this field gathered

-  ``PARAM`` is a list of documents, each corresponding to a field of
   the struct array. Note that, the while decoding, the field type can
   be inferred from the encoded value array. This list enforce the
   ordering of the fields. The inferred type shall match the field types
   encoded here.

The types given in ``PARAM`` shall match that of the actual types in
``DATA``.

.. code:: python3

    test(struct_type, struct_data, [True, False, True])


.. parsed-literal::

    
    Orignal
    
    type: struct['x: int64', 'y: float64']
    data: [(1, 4.) (2, 5.) (3, 6.)]
    mask: [True, False, True]
    
    
    Encoded
    
    {
        "d": {
            "l": {
                "$numberLong": "3"
            },
            "f": {
                "x": {
                    "d": {
                        "$binary": {
                            "base64": "GAAAACIBAAEAEgIHAJAAAwAAAAAAAAA=",
                            "subType": "00"
                        }
                    },
                    "m": {
                        "$binary": {
                            "base64": "AQAAABDg",
                            "subType": "00"
                        }
                    },
                    "t": "int64"
                },
                "y": {
                    "d": {
                        "$binary": {
                            "base64": "GAAAABEAAQAhEEAHALAAFEAAAAAAAAAYQA==",
                            "subType": "00"
                        }
                    },
                    "m": {
                        "$binary": {
                            "base64": "AQAAABDg",
                            "subType": "00"
                        }
                    },
                    "t": "float64"
                }
            }
        },
        "m": {
            "$binary": {
                "base64": "AQAAABCg",
                "subType": "00"
            }
        },
        "t": "struct",
        "p": [
            {
                "n": "x",
                "t": "int64"
            },
            {
                "n": "y",
                "t": "float64"
            }
        ]
    }
    
    
    Decoded
    
    type: struct['x: int64', 'y: float64']
    data: [(1, 4.) (2, 5.) (3, 6.)]
    mask: [ True False  True]
    


