from collections import OrderedDict

from TableMetadata import TableMetadata


class ColumnMetadata(object):

    def __init__(self, schema):
        self._date = None
        self._schema = schema
        self._queries = {}
        self._properties = {}
        self._table = None
        # properties['avg_num_elements'] = avg_num_elements
        # properties['size_bytes'] = size_bytes



    def addProperty(self, key, value):
        self._properties[key] = value

    def addQuery(self, query_name, query):
        self._queries[query_name] = query

    def set_date(self, date):
        self._date = date

    def set_table(self, table):
        self._table = table

    def __repr__(self):
        return 'ColumnMetadata{}'.format(self._queries)

    def encode(c):
        if isinstance(c, ColumnMetadata):
            parent = c._schema._parent if c._schema._parent else None
            grandparent = parent._parent if parent._parent else None

            orderedDict = OrderedDict()
            orderedDict['date']= c._date
            orderedDict['shortName']= c._schema._name_short
            orderedDict['fullName']= c._schema._name_full
            orderedDict['mode']= c._schema._mode
            orderedDict['type']= c._schema.field_type
            orderedDict['level']= c._schema._level
            orderedDict['isLeaf']= c._schema._is_leaf
            orderedDict['recordTotalBytes']= c._properties['record_bytes'] if c._properties.has_key('record_bytes') else -1
            orderedDict['recordBytesAccuracy']= c._properties['record_bytes_accuracy'] if c._properties.has_key('record_bytes_accuracy') else 'NONE'
            orderedDict['parent'] = parent._name_full if parent else None
            orderedDict['grandparent'] = grandparent._name_full if grandparent else None

            orderedDict.update(TableMetadata.encode(c._table))
            return orderedDict
        else:
            raise TypeError('Object of type {} is not JSON serializable'.format(c.__class__.__name__))
