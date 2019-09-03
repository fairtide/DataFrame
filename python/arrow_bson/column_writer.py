from .data_writer import *
import collections


def write_column(array):
    col = collections.OrderedDict()
    data = DataWriter(col)
    data.accept(array)

    return col
