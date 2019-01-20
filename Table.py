


class Table(object):

    def __init__(self, name, schema):
        self._name = name
        self._properties = {} # _num_rows, _num_bytes - i can also have them as members
        self._schema = schema


