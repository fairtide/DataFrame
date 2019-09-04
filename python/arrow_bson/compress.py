import bson
import lz4.block
import numpy
import pyarrow


def compress(data, compression_level):
    if compression_level > 0:
        buf = lz4.block.compress(data,
                                 mode='high_compression',
                                 compression=compression_level)
    else:
        buf = lz4.block.compress(data)

    return bson.Binary(buf)


def decompress(data):
    return pyarrow.py_buffer(lz4.block.decompress(data))


def encode_offsets(data):
    t = numpy.int32()
    assert len(data) % t.nbytes == 0

    n = len(data) // t.nbytes
    v = numpy.ndarray(n, t, data)

    return numpy.diff(v, prepend=numpy.int32(0)).tobytes(), v[0], v[-1] - v[0]


def encode_datetime(data, dtype):
    if len(data) == 0:
        return data

    assert len(data) % dtype.nbytes == 0

    n = len(data) // dtype.nbytes
    v = numpy.ndarray(n, dtype, data)

    return numpy.diff(v, prepend=v[0]).tobytes()


def decode_offsets(data):
    t = numpy.int32()

    assert len(data) % t.nbytes == 0

    n = len(data) // t.nbytes
    v = numpy.cumsum(numpy.ndarray(n, t, data), dtype=t)

    return pyarrow.py_buffer(v.tobytes()), n - 1


def decode_datetime(data, dtype):
    assert len(data) % dtype.nbytes == 0

    n = len(data) // dtype.nbytes
    v = numpy.cumsum(numpy.ndarray(n, dtype, data), dtype=dtype)

    return pyarrow.py_buffer(v.tobytes()), n
