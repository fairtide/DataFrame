class ArrayData(object):
    def __init__(self):
        self.type = None
        self.length = None
        self.null_count = None
        self.buffers = list()
        self.child_data = list()
