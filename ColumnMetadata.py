

class ColumnMetadata(object):

    def __init__(self, name_full, schema):
        self._name_full =  name_full
        self._schema = schema
        self._queries = {}
        self.properties = {}
        # properties['avg_num_elements'] = avg_num_elements
        # properties['size_bytes'] = size_bytes


    def addProperty(self, key, value):
        self.properties[key] = value

    def addQuery(self, query_name, query):
        self._queries[query_name] = query

    def __repr__(self):
        return 'ColumnMetadata{}'.format(self._queries)
