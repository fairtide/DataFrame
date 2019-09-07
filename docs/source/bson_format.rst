DataFrame Serialization with BSON Format
========================================

The library specify and implement (in C++ and Python) a BSON based
serialization for DataFrame data. The format is inspired by the data format of
`Arctic <https://github.com/manahl/arctic>`_, in particular its TickStore.
However, the format covers considerably more data types. The implementation
itself at the moment provides serialization to and from `Arrow
<https://arrow.apache.org>`_ tables, however, the specification is independent
of either project.

As a toy example, given a DataFrame below,

.. code-block:: python

    df = pandas.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})

is serialized to the following BSON document (printed by its `Canonical JSON Representation <https://github.com/mongodb/specifications/blob/master/source/extended-json.rst>`_).

.. code-block:: json

    {
        "x": {
            "d": { "$binary": { "base64": "GAAAACIBAAEAEgIHAJAAAwAAAAAAAAA=", "subType": "00" } },
            "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
            "t": "int64"
        },
        "y": {
            "d": { "$binary": { "base64": "AwAAADBhYmM=", "subType": "00" } },
            "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
            "t": "utf8",
            "o": { "$binary": { "base64": "EAAAAPABAAAAAAEAAAABAAAAAQAAAA==", "subType": "00" } }
        }
    }

Each column is stored as a BSON document with the its name as the key and the
corresponding array data as BSON document value. The specifics for different
array types are detailed below.

Buffers
-------

The basic building block of the data is buffers, and stored as binary in BSON
with default sub-type. Each buffers is compressed data using the `LZ4
<https://lz4.github.io/lz4/>`_ codec. The first 4 bytes of the binary is the
32-bits integer of the length of decompressed buffer. The rest of the binary is
a compressed data of `LZ4 block format
<https://github.com/lz4/lz4/blob/master/doc/lz4_Block_format.md>`_.

Unless otherwise specified, below we use the term *buffer* to refer to the
decompressed buffer instead of the compressed one stored in the BSON document.

Byte Order
----------

The data stored in the buffers are of little-endian byte-order unless specified
otherwise.

Common Fields
-------------

All array data has the following common fields:

``d`` (Data)
    The bulk of the data. Its type and structure varies for different array
    type

``m`` (Mask)
    The mask of the data to indicate missing values. The bytes within the
    buffer are MSB packed bits. Each slot corresponding to an array element.

``t`` (Type)
    A single string denotes the type of the array

Some array types may have additional fields,

``p`` (Parameter)
    Used by parametric typed array to store informations of the array type.

A data type may also be serialized as a BSON document and referred below. It is
the subset of the ``t`` and ``p`` (if exist) fields of the BSON document of a
serialized array.

Offsets Buffer
--------------

Offsets or element counts are stored in a buffer of 32-bit integers of the
length each element in variable sized arrays (such as lists and strings). The
first element is always 0, followed by the length of each element in the array.
Therefore its length is the array length plus 1. The vector of its cumulative
sums is thus the offsets of each array elements.

For example, a list of lists,

.. code-block::

    v = [[1, 2, 3], [4], [5, 6]]

will be encoded by two lists,

.. code-block::

    d = [1, 2, 3, 4, 5, 6]

and

.. code-block::

    o = [0, 3, 1, 2]

To restore the original list of lists, one use the following simple logic,

.. code-block::

    begin, end = o[:2]
    u = list()
    for s in o[1:]:
        u.append(d[begin:end])
        begin = end
        end += s

To gain random access to the data without copying, compute the offset array
first. The reason for not storing the offset array itself is that usually
the distinct values of element length is limitted and thus the compressed
data become smaller, while the offset array itself is a monotically
non-decreasing sequeces.

Primitive Arrays
----------------

Required Fields
    * ``d``: The raw buffer of values
    * ``m``: The mask
    * ``t``: The data type

Primitive arrays contains integral or floating point data. Each element of the
array has fixed byte width, which is listed below

======= ==========
Type    Byte width
======= ==========
bool    1
int8    1
int16   2
int32   4
int64   8
uint8   1
uint16  2
uint32  4
uint64  8
float16 2
float32 4
float64 8
======= ==========

Example (``int32`` array):

.. code-block:: json

    {
        "d": { "$binary": { "base64": "DAAAAMCvTEJazvY/LjU7hZE=", "subType": "00" } },
        "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
        "t": "int32"
    }


Date Arrays
-----------

Required Fields
    * ``d``: The raw buffer of difference encoded values
    * ``m``: The mask
    * ``t``: The data type

There are two types for date arrays. ``date[d]`` and ``date[ms]``, with value
type 32-bits integers 64-bits integers, respectively.

Date arrays are similar to primitive arrays except that the values are
difference encoded. For example, given original values of days since UNIX
epoch,

.. code-block::

    [1, 3, 5, 7, 8, 9, 10, 8]

The values stored are

.. code-block::

    [1, 2, 1, 2, 1, 1, 1, -2]

That is, the first element is the original value, and each element that follows
is the difference between he original value and its predecessor. The rationale
behind such encoding is that the compressed data is much smaller for some
commonly occurring data such as a sequence of monotonically increasing, evenly
spaced dates, which is common in finance data. For random data there no
advantage or disadvantage in terms of space on average. The cost of encoding or
decoding is small or negligible compared to the cost of compression or
decompression, respectively.

Timestamp Array
---------------

Required Fields
    * ``d``: The raw buffer of diff encoded values
    * ``m``: The mask
    * ``t``: The data type

Optional Fields
    * ``p``: String of time zone

Timestamp array is similar to date arrays in that its values (64-bits integers)
are also difference encoded. There are four timestamp types, ``timestamp[s]``,
``timestamp[ms]``, ``timestamp[us]``, and ``timestamp[ns]``, intended for use
of timestamps with precisions seconds, milliseconds, microseconds, and
nanoseconds, respectively.

Time Arrays
-----------

Required Fields
    * ``d``: The raw buffer of diff encoded values
    * ``m``: The mask
    * ``t``: The data type

Time arrays are values of time of day. There are four time types, ``time[s]``,
``time[ms]``, ``time[us]``, and ``time[ns]``, intended for use of timestamps
with precisions seconds, milliseconds, microseconds, and nanoseconds,
respectively. The first two has 32-bits integers as its values and last two has
64-bits integers as its values.

Null Array
----------

Required Fields
    * ``d``: 64-bits integer of array length
    * ``m``: The mask. Filled with zero byes.
    * ``t``: The data type (``null``)

Example:

.. code-block:: json

    {
        "d": { "$numberLong": "3" },
        "m": { "$binary": { "base64": "AQAAABAA", "subType": "00" } },
        "t": "null"
    }

Binary and String Array
-----------------------

Required Fields
    * ``d``: The raw buffer concatenated data
    * ``m``: The mask
    * ``t``: The data type (``bytes`` or ``utf8``)
    * ``o``: The offsets

Binary (``bytes``) or string (``utf8``) arrays are identical in their memory
layout. The data buffer is the raw bytes of each element concatenated together.
The offset buffer is as described above, used to delimit the data buffer.

Opaque Array
------------

Required Fields
    * ``d``: The raw buffer concatenated data
    * ``m``: The mask
    * ``t``: The data type (``opaque``)
    * ``p``: 32-bits integer of byte width

An opaque typed array is similar to a binary array except that the byte width
of each element is fixed and thus there's no need to encode the offsets.

Factor and Ordered Array
------------------------

Required Fields
    * ``d``: The data (see below)
    * ``m``: The mask
    * ``t``: The data type (``factor`` or ``ordered``)
    * ``p``: Index and value types (see below)

``factor`` and ``ordered`` are both dictionary types. Some languages support
both (such as R from which the names are taken), while others does not make any
distinction between the two. The layout of the document is identical to both.

The data field is a BSON document composed of two fields

``i`` (Index)
    The index array serialized as BSON document
``d`` (Dictionary)
    The dictionary value array serialized as BSON document

The parameter field is a BSON document composed of two fields

``i`` (Index)
    The index type serialized as BSON document.

``d`` (Dictionary)
    The dictionary value value serialized as BSON document

Example:

.. code-block:: json

    {
        "d": {
            "i": {
                "d": { "$binary": { "base64": "DAAAAMAJAAAAAQAAAAcAAAA=", "subType": "00" } },
                "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
                "t": "int32"
            },
            "d": {
                "d": { "$binary": { "base64": "IAAAAPARH7JcmE1LzE1uaHRTEAro9wkrvQk7FUkmXANkMO7nKUg=", "subType": "00" } },
                "m": { "$binary": { "base64": "AgAAACD/wA==", "subType": "00" } },
                "t": "utf8",
                "o": { "$binary": { "base64": "LAAAAFMAAAAABAQAkwMAAAABAAAABggAFgIIAFAACAAAAA==", "subType": "00" } }
            }
        },
        "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
        "t": "ordered",
        "p": {
            "i": { "t": "int32" },
            "d": { "t": "utf8" }
        }
    }

List Array
----------

Required Fields
    * ``d``: The concatenated value array serialized as a BSON document
    * ``m``: The mask
    * ``t``: The data type (``list``)
    * ``p``: The value type serialized as a BSON document
    * ``o``: The offsets

Example:

.. code-block:: json

    {
        "d": {
            "d": { "$binary": { "base64": "UAAAAPBBmYzN7kSpfPmZEXRK7BBM0DjPJWCZ4UH7kAuc+bDQ+gkhz5yl0DQCKZt3bDJFfR67Ut5UhW4pKAEk8GzlEjcvUjfVGlbF1NtRRdME+FkIcOs=", "subType": "00" } },
            "m": { "$binary": { "base64": "AwAAADD///A=", "subType": "00" } },
            "t": "int32"
        },
        "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
        "t": "list",
        "p": { "t": "int32" },
        "o": { "$binary": { "base64": "EAAAAPABAAAAAAQAAAAJAAAABwAAAA==", "subType": "00" } }
    }


Struct Array
------------

Required Fields
    * ``d``: The data (see below)
    * ``m``: The mask
    * ``t``: The data type (``struct``)
    * ``p``: The fields (see below)

The data field has the following subfields

* ``l``: The length of the structure array
* ``f``: Data of each fields. Each element is an array of all values in a
  given field, with field name as the key and serialized BSON document as the
  value.

The parameter field is an array of documents, each element is the field type
serialized as a BSON document, plus a field ``n``, the name of the field.

Example:

.. code-block:: json

    {
        "d": {
            "l": { "$numberLong": "3" },
            "f": {
                "x": {
                    "d": { "$binary": { "base64": "DAAAAMCQMFbTLMBdM04UP74=", "subType": "00" } },
                    "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
                    "t": "int32"
                },
                "y": {
                    "d": { "$binary": { "base64": "DAAAAMCTai8/ys9UPhTufD8=", "subType": "00" } },
                    "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
                    "t": "float32"
                }
            }
        },
        "m": { "$binary": { "base64": "AQAAABDg", "subType": "00" } },
        "t": "struct",
        "p": [{ "n": "x", "t": "int32" }, { "n": "y", "t": "float32" }]
    }
