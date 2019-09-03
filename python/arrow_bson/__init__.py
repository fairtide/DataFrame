from .column_reader import *
import bson.raw_bson
import pyarrow


def read_table(buf):
    doc = bson.raw_bson.RawBSONDocument(buf)
    data = list()
    for k, v in doc.items():
        data.append(pyarrow.column(k, read_column(v)))

    return pyarrow.Table.from_arrays(data)
