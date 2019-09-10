import lz4.block
import numpy


def compress(data: bytes, compression_level=0) -> bytes:
    if compression_level > 0:
        return lz4.block.compress(data,
                                  mode='high_compression',
                                  compression=compression_level)
    else:
        return lz4.block.compress(data)


def decompress(data: bytes) -> bytes:
    return lz4.block.decompress(data)


def encode_offsets(data: numpy.ndarray) -> numpy.ndarray:
    return numpy.diff(data, prepend=data[0])


def decode_offsets(data: numpy.ndarray) -> numpy.ndarray:
    return numpy.cumsum(data, dtype=data.dtype)


def encode_datetime(data: numpy.ndarray) -> numpy.ndarray:
    return numpy.diff(data, prepend=data[0] - data[0])


def decode_datetime(data: numpy.ndarray) -> numpy.ndarray:
    return numpy.cumsum(data, dtype=data.dtype)
