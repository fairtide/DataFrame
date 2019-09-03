from .schema import *
import pyarrow


def read_type(doc):
    t = doc[TYPE]

    if PARAM in doc:
        tp = doc[PARAM]
    else:
        tp = None

    if t == 'null':
        return pyarrow.null()

    if t == 'bool':
        return pyarrow.bool_()

    if t == 'int8':
        return pyarrow.int8()

    if t == 'int16':
        return pyarrow.int16()

    if t == 'int32':
        return pyarrow.int32()

    if t == 'int64':
        return pyarrow.int64()

    if t == 'uint8':
        return pyarrow.uint8()

    if t == 'uint16':
        return pyarrow.uint16()

    if t == 'uint32':
        return pyarrow.uint32()

    if t == 'uint64':
        return pyarrow.uint64()

    if t == 'float16':
        return pyarrow.float16()

    if t == 'float32':
        return pyarrow.float32()

    if t == 'float64':
        return pyarrow.float64()

    if t == 'date[d]':
        return pyarrow.date32()

    if t == 'date[ms]':
        return pyarrow.date64()

    if t == 'timestamp[s]':
        return pyarrow.timestamp('s')

    if t == 'timestamp[ms]':
        return pyarrow.timestamp('ms')

    if t == 'timestamp[us]':
        return pyarrow.timestamp('us')

    if t == 'timestamp[ns]':
        return pyarrow.timestamp('ns')

    if t == 'time[s]':
        return pyarrow.time32('s')

    if t == 'time[ms]':
        return pyarrow.time32('ms')

    if t == 'time[us]':
        return pyarrow.time64('us')

    if t == 'time[ns]':
        return pyarrow.time64('ns')

    if t == 'utf8':
        return pyarrow.utf8()

    if t == 'bytes':
        return pyarrow.binary()

    if t == 'factor':
        if tp is None:
            index_type = pyarrow.int32()
            dict_type = pyarrow.utf8()
        else:
            index_type = read_type(tp[INDEX])
            dict_type = read_type(tp[DICT])
        return pyarrow.dictionary(index_type, dict_type, False)

    if t == 'ordered':
        if tp is None:
            index_type = pyarrow.int32()
            dict_type = pyarrow.utf8()
        else:
            index_type = read_type(tp[INDEX])
            dict_type = read_type(tp[DICT])
        return pyarrow.dictionary(index_type, dict_type, True)

    if t == 'pod':
        return pyarrow.binary(tp)

    if t == 'decimal':
        return pyarrow.decimal(tp[PRECISION], tp[SCALE])

    if t == 'list':
        return pyarrow.list_(read_type(tp))

    if t == 'struct':
        return pyarrow.struct(
            [pyarrow.field(f[NAME], read_type(f)) for f in tp])

    raise ValueError(f'{t} is not supported BSON DataFrame type')
