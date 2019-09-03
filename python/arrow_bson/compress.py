import lz4.block
import numpy
import pyarrow


def decompress(data):
    # TODO
    return pyarrow.py_buffer(lz4.block.decompress(data))


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
