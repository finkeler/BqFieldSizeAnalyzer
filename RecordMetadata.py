from collections import OrderedDict

from TableMetadata import TableMetadata


class RecordMetadata(object):

    def __init__(self, schema):
        self._date = None
        self._schema = schema
        self._queries = {}
        self._properties = {}
        self._table = None
        # properties['avg_num_elements'] = avg_num_elements
        # properties['size_bytes'] = size_bytes

    def add_property(self, key, value):
        self._properties[key] = value

    def add_query(self, query_name, query):
        self._queries[query_name] = query

    def set_date(self, date):
        self._date = date

    def set_table(self, table):
        self._table = table

    def __repr__(self):
        return 'RecordMetadata{}'.format(self._queries)

    def encode(c):
        if isinstance(c, RecordMetadata):
            parent = c._schema._parent if c._schema._parent else None
            grandparent = parent._parent if parent._parent else None

            ordered_dict = OrderedDict()
            ordered_dict['date']= c._date
            ordered_dict['shortName']= c._schema._name_short
            ordered_dict['fullName']= c._schema._name_full
            ordered_dict['mode']= c._schema._mode
            ordered_dict['type']= c._schema.field_type
            ordered_dict['level']= c._schema._level
            ordered_dict['isLeaf']= c._schema._is_leaf
            ordered_dict['recordTotalBytes']= c._properties['record_bytes'] if c._properties.has_key('record_bytes') else -1
            ordered_dict['recordBytesAccuracy']= c._properties['record_bytes_accuracy'] if c._properties.has_key('record_bytes_accuracy') else 'None'
            ordered_dict['parent'] = parent._name_full if parent else 'None'
            ordered_dict['grandparent'] = grandparent._name_full if grandparent else 'None'

            ordered_dict.update(TableMetadata.encode(c._table))
            return ordered_dict
        else:
            raise TypeError('Object of type {} is not JSON serializable'.format(c.__class__.__name__))
