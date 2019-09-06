import pyarrow


class BaseVisitor(object):
    def accept_type(self, typ, val):
        if pyarrow.types.is_null(typ):
            return self.visit_null(val)

        if pyarrow.types.is_boolean(typ):
            return self.visit_bool(val)

        if pyarrow.types.is_int8(typ):
            return self.visit_int8(val)

        if pyarrow.types.is_int16(typ):
            return self.visit_int16(val)

        if pyarrow.types.is_int32(typ):
            return self.visit_int32(val)

        if pyarrow.types.is_int64(typ):
            return self.visit_int64(val)

        if pyarrow.types.is_uint8(typ):
            return self.visit_uint8(val)

        if pyarrow.types.is_uint16(typ):
            return self.visit_uint16(val)

        if pyarrow.types.is_uint32(typ):
            return self.visit_uint32(val)

        if pyarrow.types.is_uint64(typ):
            return self.visit_uint64(val)

        if pyarrow.types.is_float16(typ):
            return self.visit_float16(val)

        if pyarrow.types.is_float32(typ):
            return self.visit_float32(val)

        if pyarrow.types.is_float64(typ):
            return self.visit_float64(val)

        if pyarrow.types.is_date32(typ):
            return self.visit_date32(val)

        if pyarrow.types.is_date64(typ):
            return self.visit_date64(val)

        if pyarrow.types.is_timestamp(typ):
            return self.visit_timestamp(val)

        if pyarrow.types.is_time32(typ):
            return self.visit_time32(val)

        if pyarrow.types.is_time64(typ):
            return self.visit_time64(val)

        if pyarrow.types.is_string(typ):
            return self.visit_string(val)

        if pyarrow.types.is_binary(typ):
            return self.visit_binary(val)

        if pyarrow.types.is_fixed_size_binary(typ):
            return self.visit_fixed_size_binary(val)

        if pyarrow.types.is_dictionary(typ):
            return self.visit_dictionary(val)

        if pyarrow.types.is_list(typ):
            return self.visit_list(val)

        if pyarrow.types.is_struct(typ):
            return self.visit_struct(val)

        raise NotImplementedError(typ)

    def visit_null(self, val) -> None:
        raise NotImplementedError()

    def visit_bool(self, val) -> None:
        raise NotImplementedError()

    def visit_int8(self, val) -> None:
        raise NotImplementedError()

    def visit_int16(self, val) -> None:
        raise NotImplementedError()

    def visit_int32(self, val) -> None:
        raise NotImplementedError()

    def visit_int64(self, val) -> None:
        raise NotImplementedError()

    def visit_uint8(self, val) -> None:
        raise NotImplementedError()

    def visit_uint16(self, val) -> None:
        raise NotImplementedError()

    def visit_uint32(self, val) -> None:
        raise NotImplementedError()

    def visit_uint64(self, val) -> None:
        raise NotImplementedError()

    def visit_float32(self, val) -> None:
        raise NotImplementedError()

    def visit_float64(self, val) -> None:
        raise NotImplementedError()

    def visit_date32(self, val) -> None:
        raise NotImplementedError()

    def visit_date64(self, val) -> None:
        raise NotImplementedError()

    def visit_string(self, val) -> None:
        raise NotImplementedError()

    def visit_binary(self, val) -> None:
        raise NotImplementedError()

    def visit_timestamp(self, val) -> None:
        raise NotImplementedError()

    def visit_time32(self, val) -> None:
        raise NotImplementedError()

    def visit_time64(self, val) -> None:
        raise NotImplementedError()

    def visit_fixed_size_binary(self, val) -> None:
        raise NotImplementedError()

    def visit_dictionary(self, val) -> None:
        raise NotImplementedError()

    def visit_list(self, val) -> None:
        raise NotImplementedError()

    def visit_struct(self, val) -> None:
        raise NotImplementedError()


class TypeVisitor(BaseVisitor):
    def accept(self, typ: pyarrow.DataType) -> None:
        return self.accept_type(typ, typ)


class ArrayVisitor(BaseVisitor):
    def accept(self, array: pyarrow.Array) -> None:
        return self.accept_type(array.type, array)
