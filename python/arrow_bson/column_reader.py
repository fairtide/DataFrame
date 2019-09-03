from .array_data import *
from .data_reader import *
from .type_reader import *


def read_column(doc):
    data = ArrayData()
    data.type = read_type(doc)
    reader = DataReader(data, doc)
    reader.accept(data.type)

    return data.make_array()
