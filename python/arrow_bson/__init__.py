from .array_data import *
from .data_reader import *
from .data_writer import *
from .type_reader import *
import bson.raw_bson
import collections
import pyarrow


def read_column(doc):
    data = ArrayData()
    data.type = read_type(doc)
    reader = DataReader(data, doc)
    reader.accept(data.type)

    return data.make_array()


def write_column(array, compression_level):
    col = collections.OrderedDict()
    writer = DataWriter(col, compression_level)
    writer.accept(array)

    return col


def read_table(buf):
    doc = bson.raw_bson.RawBSONDocument(buf)
    data = list()
    for k, v in doc.items():
        data.append(pyarrow.column(k, read_column(v)))

    return pyarrow.Table.from_arrays(data)


def write_table(table, compression_level=0):
    table = table.combine_chunks()
    doc = collections.OrderedDict()
    for col in table.columns:
        doc[col.name] = write_column(col.data.chunks[0], compression_level)

    return bson.encode(doc)


def read_dataframe(buf, **kwargs):
    return read_table(buf).to_pandas(**kwargs)


def write_dataframe(data, preserve_index=False, **kwargs):
    return write_table(
        pyarrow.from_pandas(data, preserve_index=preserve_index, **kwargs))
