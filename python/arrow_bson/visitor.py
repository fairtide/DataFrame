import pyarrow


class TypeVisitor(object):
    def accept(self, typ: pyarrow.DataType) -> None:
        if typ.equals(pyarrow.null()):
            return self.visit_null(typ)

        if typ.equals(pyarrow.bool_()):
            return self.visit_bool(typ)

        if typ.equals(pyarrow.int8()):
            return self.visit_int8(typ)

        if typ.equals(pyarrow.int16()):
            return self.visit_int16(typ)

        if typ.equals(pyarrow.int32()):
            return self.visit_int32(typ)

        if typ.equals(pyarrow.int64()):
            return self.visit_int64(typ)

        if typ.equals(pyarrow.uint8()):
            return self.visit_uint8(typ)

        if typ.equals(pyarrow.uint16()):
            return self.visit_uint16(typ)

        if typ.equals(pyarrow.uint32()):
            return self.visit_uint32(typ)

        if typ.equals(pyarrow.uint64()):
            return self.visit_uint64(typ)

        if typ.equals(pyarrow.float16()):
            return self.visit_float16(typ)

        if typ.equals(pyarrow.float32()):
            return self.visit_float32(typ)

        if typ.equals(pyarrow.float64()):
            return self.visit_float64(typ)

        if typ.equals(pyarrow.date32()):
            return self.visit_date32(typ)

        if typ.equals(pyarrow.date64()):
            return self.visit_date64(typ)

        if typ.equals(pyarrow.utf8()):
            return self.visit_utf8(typ)

        if typ.equals(pyarrow.binary()):
            return self.visit_binary(typ)

        if isinstance(typ, pyarrow.TimestampType):
            return self.visit_timestamp(typ)

        if isinstance(typ, pyarrow.Time32Type):
            return self.visit_time32(typ)

        if isinstance(typ, pyarrow.Time64Type):
            return self.visit_time64(typ)

        if isinstance(typ, pyarrow.FixedSizeBinaryType):
            return self.visit_fixed_size_binary(typ)

        if isinstance(typ, pyarrow.DictionaryType):
            return self.visit_dictionary(typ)

        if isinstance(typ, pyarrow.ListType):
            return self.visit_list(typ)

        if isinstance(typ, pyarrow.StructType):
            return self.visit_struct(typ)

        raise NotImplementedError(typ)

    def visit_null(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_bool(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_int8(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_int16(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_int32(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_int64(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_uint8(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_uint16(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_uint32(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_uint64(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_float32(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_float64(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_date32(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_date64(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_utf8(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_binary(self, typ: pyarrow.DataType) -> None:
        raise NotImplementedError(typ)

    def visit_timestamp(self, typ: pyarrow.TimestampType) -> None:
        raise NotImplementedError(typ)

    def visit_time32(self, typ: pyarrow.Time32Type) -> None:
        raise NotImplementedError(typ)

    def visit_time64(self, typ: pyarrow.Time64Type) -> None:
        raise NotImplementedError(typ)

    def visit_fixed_size_binary(self,
                                typ: pyarrow.FixedSizeBinaryType) -> None:
        raise NotImplementedError(typ)

    def visit_dictionary(self, typ: pyarrow.DictionaryType) -> None:
        raise NotImplementedError(typ)

    def visit_decimal(self, typ: pyarrow.Decimal128Type) -> None:
        raise NotImplementedError(typ)

    def visit_list(self, typ: pyarrow.ListType) -> None:
        raise NotImplementedError(typ)

    def visit_struct(self, typ: pyarrow.StructType) -> None:
        raise NotImplementedError(typ)


class ArrayVisitor(object):
    def accept(self, array: pyarrow.Array) -> None:
        typ = array.type

        if isinstance(array, pyarrow.NullArray):
            return self.visit_null(typ)

        if isinstance(array, pyarrow.BooleanArray):
            return self.visit_bool(typ)

        if isinstance(array, pyarrow.Int8Array):
            return self.visit_int8(typ)

        if isinstance(array, pyarrow.Int16Array):
            return self.visit_int16(typ)

        if isinstance(array, pyarrow.Int32Array):
            return self.visit_int32(typ)

        if isinstance(array, pyarrow.Int64Array):
            return self.visit_int64(typ)

        if isinstance(array, pyarrow.UInt8Array):
            return self.visit_uint8(typ)

        if isinstance(array, pyarrow.UInt16Array):
            return self.visit_uint16(typ)

        if isinstance(array, pyarrow.UInt32Array):
            return self.visit_uint32(typ)

        if isinstance(array, pyarrow.UInt64Array):
            return self.visit_uint64(typ)

        if typ.equals(pyarrow.float16()):
            return self.visit_float16(typ)

        if typ.equals(pyarrow.float32()):
            return self.visit_float32(typ)

        if typ.equals(pyarrow.float64()):
            return self.visit_float64(typ)

        if isinstance(array, pyarrow.Date32Array):
            return self.visit_date32(typ)

        if isinstance(array, pyarrow.Date64Array):
            return self.visit_date64(typ)

        if isinstance(array, pyarrow.StringArray):
            return self.visit_utf8(typ)

        if isinstance(array, pyarrow.BinaryArray):
            return self.visit_binary(typ)

        if isinstance(typ, pyarrow.TimestampType):
            return self.visit_timestamp(typ)

        if isinstance(typ, pyarrow.Time32Type):
            return self.visit_time32(typ)

        if isinstance(typ, pyarrow.Time64Type):
            return self.visit_time64(typ)

        if isinstance(typ, pyarrow.FixedSizeBinaryArray):
            return self.visit_fixed_size_binary(typ)

        if isinstance(typ, pyarrow.DictionaryArray):
            return self.visit_dictionary(typ)

        if isinstance(typ, pyarrow.ListArray):
            return self.visit_list(typ)

        if isinstance(typ, pyarrow.StructArray):
            return self.visit_struct(typ)

        raise NotImplementedError(type(array))

    def visit_null(self, array: pyarrow.NullArray) -> None:
        raise NotImplementedError(type(array))

    def visit_bool(self, array: pyarrow.BooleanArray) -> None:
        raise NotImplementedError(type(array))

    def visit_int8(self, array: pyarrow.Int8Array) -> None:
        raise NotImplementedError(type(array))

    def visit_int16(self, array: pyarrow.Int16Array) -> None:
        raise NotImplementedError(type(array))

    def visit_int32(self, array: pyarrow.Int32Array) -> None:
        raise NotImplementedError(type(array))

    def visit_int64(self, array: pyarrow.Int64Array) -> None:
        raise NotImplementedError(type(array))

    def visit_uint8(self, array: pyarrow.UInt8Array) -> None:
        raise NotImplementedError(type(array))

    def visit_uint16(self, array: pyarrow.UInt16Array) -> None:
        raise NotImplementedError(type(array))

    def visit_uint32(self, array: pyarrow.UInt32Array) -> None:
        raise NotImplementedError(type(array))

    def visit_uint64(self, array: pyarrow.UInt64Array) -> None:
        raise NotImplementedError(type(array))

    def visit_float16(self, array: pyarrow.FloatingPointArray) -> None:
        raise NotImplementedError(type(array))

    def visit_float32(self, array: pyarrow.FloatingPointArray) -> None:
        raise NotImplementedError(type(array))

    def visit_float64(self, array: pyarrow.FloatingPointArray) -> None:
        raise NotImplementedError(type(array))

    def visit_date32(self, array: pyarrow.Date32Array) -> None:
        raise NotImplementedError(type(array))

    def visit_date64(self, array: pyarrow.Date64Array) -> None:
        raise NotImplementedError(type(array))

    def visit_utf8(self, array: pyarrow.StringArray) -> None:
        raise NotImplementedError(type(array))

    def visit_binary(self, array: pyarrow.BinaryArray) -> None:
        raise NotImplementedError(type(array))

    def visit_timestamp(self, array: pyarrow.TimestampArray) -> None:
        raise NotImplementedError(type(array))

    def visit_time32(self, array: pyarrow.Time32Array) -> None:
        raise NotImplementedError(type(array))

    def visit_time64(self, array: pyarrow.Time64Array) -> None:
        raise NotImplementedError(type(array))

    def visit_fixed_size_binary(self,
                                array: pyarrow.FixedSizeBinaryArray) -> None:
        raise NotImplementedError(type(array))

    def visit_dictionary(self, array: pyarrow.DictionaryArray) -> None:
        raise NotImplementedError(type(array))

    def visit_decimal(self, array: pyarrow.Decimal128Array) -> None:
        raise NotImplementedError(type(array))

    def visit_list(self, array: pyarrow.ListArray) -> None:
        raise NotImplementedError(type(array))

    def visit_struct(self, array: pyarrow.StructArray) -> None:
        raise NotImplementedError(type(array))
