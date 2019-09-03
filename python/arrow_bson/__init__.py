from .column_reader import *
from .column_writer import *
import collections
import bson.raw_bson
import pyarrow


def read_table(buf):
    doc = bson.raw_bson.RawBSONDocument(buf)
    data = list()
    for k, v in doc.items():
        data.append(pyarrow.column(k, read_column(v)))

    return pyarrow.Table.from_arrays(data)


def write_table(table):
    table = table.combine_chunks()
    doc = collections.OrdereDict()
    for col in table.columns:
        doc[col.name] = write_column(col.data.chunks[0])
    return bson.encode(doc)
