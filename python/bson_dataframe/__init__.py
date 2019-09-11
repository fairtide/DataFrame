from .array_data import *
from .compress import *
from .data_reader import *
from .data_writer import *
from .schema import *
from .type_reader import *
from .type_writer import *
from .visitor import *

import bson
import bson.raw_bson
import pyarrow


def write_array(array: pyarrow.Array, compression_level=0) -> bytes:
    doc = write_data({}, array, compression_level)

    return bson.encode(doc)


def write_table(table: pyarrow.Table, compression_level=0) -> bytes:
    table = table.combine_chunks()
    doc = {}
    for col in table.columns:
        buf = write_array(col.data.chunks[0], compression_level)
        doc[col.name] = bson.raw_bson.RawBSONDocument(buf)

    return bson.encode(doc)


def read_array(doc) -> pyarrow.Array:
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    return read_data(doc).make_array()


def read_table(doc) -> pyarrow.Table:
    if isinstance(doc, bytes):
        doc = bson.raw_bson.RawBSONDocument(doc)

    data = [pyarrow.column(k, read_array(v)) for k, v in doc.items()]

    return pyarrow.Table.from_arrays(data)
