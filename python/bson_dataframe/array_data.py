import pyarrow


class ArrayData(object):
    def __init__(self):
        self.type = None
        self.length = None
        self.null_count = None
        self.buffers = []
        self.children = []
        self.dictionary = None

    def make_array(self):
        if isinstance(self.type, pyarrow.DictionaryType):
            if isinstance(self.dictionary, ArrayData):
                values = self.dictionary.make_array()
            else:
                values = self.dictionary

            return pyarrow.DictionaryArray.from_arrays(
                self._make_array(self.type.index_type),
                values,
                ordered=self.type.ordered)
        else:
            return self._make_array(self.type)

    def _make_array(self, typ):
        return pyarrow.Array.from_buffers(typ,
                                          self.length,
                                          self.buffers,
                                          self.null_count,
                                          children=self.children)
