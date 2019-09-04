from .column_reader import *
from .column_writer import *
import bson.raw_bson
import collections
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


def read_dataframe(buf, **kwargs):
    return read_table(buf).to_pandas(**kwargs)


def write_dataframe(data, preserve_index=False, **kwargs):
    return write_table(
        pyarrow.from_pandas(data, preserve_index=preserve_index, **kwargs))
