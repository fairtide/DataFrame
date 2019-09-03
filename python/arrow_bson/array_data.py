import pyarrow


class ArrayData(object):
    def __init__(self):
        self.type = None
        self.length = None
        self.null_count = None
        self.buffers = list()
        self.children = list()
        self.dictionary = None

    def make_array(self):
        if isinstance(self.type, pyarrow.DictionaryType):
            return pyarrow.DictionaryArray.from_arrays(
                self._make_array(self.type.index_type),
                self.dictionary._make_array(self.type.value_type),
                ordered=self.type.ordered)
        else:
            return self._make_array(self.type)

    def _make_array(self, typ):
        return pyarrow.Array.from_buffers(typ,
                                          self.length,
                                          self.buffers,
                                          self.null_count,
                                          children=self.children)
