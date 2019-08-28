from .schema import *
from .array_data import *
from .type_reader import *
import lz4.block
import numpy
import pyarrow


def _decompress(data):
    return lz4.block.decompress(data)


def _decode_offsets(data):
    t = numpy.int32()
    n = len(data) // t.nbytes
    v = numpy.cumsum(numpy.ndarray(n, t, data), dtype=t)

    return v.data.tobytes(), n - 1


def _decode_datetime(data, dtype):
    n = len(data) // dtype.nbytes
    v = numpy.cumsum(numpy.ndarray(n, dtype, data), dtype=dtype)

    return v.data.tobytes()


def _make_mask(data, doc):
    if data.length == 0:
        data.null_count = 0
        return None

    bits = _decompress(doc[MASK])
    vals = numpy.unpackbits(numpy.ndarray(len(bits), numpy.uint8, bits))
    data.null_count = data.length - numpy.sum(vals)

    if data.null_count == 0:
        return None

    return numpy.packbits(vals, bitorder='little').data.tobytes()


def read_data(doc):
    d = doc[DATA]
    t = doc[TYPE]

    data = ArrayData()
    data.type = read_type(doc)

    if t == 'null':
        data.length = d
        data.null_count = data.length
        data.buffers.append(None)

    elif t == 'bool':
        vals = _decompress(d)
        n = len(vals)
        p = numpy.ndarray(n, numpy.uint8, vals)
        bits = numpy.packbits(p, bitorder='little')

        data.length = n
        data.buffers.append(_make_mask(data, doc))
        data.buffers.append(bits)

    elif t in ('utf8', 'bytes'):
        offsets, length = _decode_offsets(_decompress(doc[OFFSET]))

        data.length = length
        data.buffers.append(_make_mask(data, doc))
        data.buffers.append(offsets)
        data.buffers.append(_decompress(d))

    elif t == 'list':
        offsets, length = _decode_offsets(_decompress(doc[OFFSET]))

        data.length = length
        data.buffers.append(_make_mask(data, doc))
        data.buffers.append(offsets)
        data.child_data.append(read_data(d))

    elif t == 'struct':
        l = d[LENGTH]
        f = d[FIELDS]

        data.length = l
        data.buffers.append(_make_mask(data, doc))
        for field_type in doc[PARAM]:
            data.child_data.append(read_data(f[field_type[NAME]]))

    elif t in ('factor', 'ordered'):
        return pyarrow.DictionaryArray.from_arrays(read_data(d[INDEX]),
                                                   read_data(d[DICT]),
                                                   ordered=data.type.ordered)
    else:  # only flat types shall remain here
        values = _decompress(doc[DATA])
        if t == 'date[d]':
            values = _decode_datetime(values, numpy.int32())
        elif t == 'date[ms]' or isinstance(data.type, pyarrow.TimestampType):
            values = _decode_datetime(values, numpy.int64())

        data.length = len(values) // (data.type.bit_width // 8)
        data.buffers.append(_make_mask(data, doc))
        data.buffers.append(values)

    data.buffers = [
        x if x is None else pyarrow.py_buffer(x) for x in data.buffers
    ]

    return pyarrow.Array.from_buffers(data.type,
                                      data.length,
                                      data.buffers,
                                      data.null_count,
                                      children=data.child_data)
