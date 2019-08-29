from .data_reader import *
import collections
import bson.raw_bson


def read_table(buf):
    doc = bson.raw_bson.RawBSONDocument(buf)
    data = collections.OrderedDict()
    for k, v in doc.items():
        data[k] = read_data(v)

    return pyarrow.table(data)
