from collections import OrderedDict

from TableMetadata import TableMetadata


class RecordMetadata(object):

    def __init__(self,  name_short, name_full, field_type, level, parent=None, alias=None,mode='NULLABLE',
                        description=None, fields=(), is_leaf=True, is_local_top=False, is_global_top=False, is_dummy=False, date=None, start_of_week=None,
                        queries=None, properties=None, table=None):
        self._name_short = name_short
        self._name_full = name_full
        self._parent = parent
        self._level = level
        self._field_type = field_type
        self._mode = mode
        self._description = description
        self._fields = tuple(fields)
        self._is_leaf = is_leaf
        self._is_local_top = is_local_top
        self._is_global_top = is_global_top
        self._is_dummy = is_dummy
        self._date = date
        self._start_of_week = start_of_week
        if queries is None:
            self._queries = {}
        else:
            self._queries = queries

        if properties is None:
            self._properties = {}
        else:
            self._properties = properties
        self._table = table
        # properties['avg_num_elements'] = avg_num_elements
        # properties['size_bytes'] = size_bytes

    def add_property(self, key, value):
        self._properties[key] = value

    def add_query(self, query_name, query):
        self._queries[query_name] = query

    def set_date(self, date):
        self._date = date

    def set_start_of_week(self, start_of_week):
        self._start_of_week = start_of_week

    def set_table(self, table):
        self._table = table

    def __repr__(self):
        return 'RecordMetadata{}'.format(self._queries)

    def encode(c):
        if isinstance(c, RecordMetadata):
            parent = c._parent
            grandparent = c._parent._parent if c._parent else None

            ordered_dict = OrderedDict()
            ordered_dict['date']= c._date
            ordered_dict['start_of_week'] = c._start_of_week
            ordered_dict['shortName']= c._name_short
            ordered_dict['fullName']= c._name_full
            ordered_dict['mode']= c._mode
            ordered_dict['type']= c._field_type
            ordered_dict['level']= c._level
            ordered_dict['isLeaf']= c._is_leaf
            ordered_dict['isLocalTop'] = c._is_local_top
            ordered_dict['isGlobalTop'] = c._is_global_top
            ordered_dict['isDummy'] = c._is_dummy
            ordered_dict['recordTotalBytes']= c._properties['record_bytes'] if c._properties.has_key('record_bytes') else -1
            ordered_dict['recordBytesAccuracy']= c._properties['record_bytes_accuracy'] if c._properties.has_key('record_bytes_accuracy') else 'None'
            ordered_dict['parent'] = parent._name_full if parent else 'None'
            #ordered_dict['parenTotalBytes'] = parent._properties['record_bytes'] if parent and parent._properties.has_key('record_bytes') else -1
            ordered_dict['grandparent'] = grandparent._name_full if grandparent else 'None'
            #ordered_dict['grandparentTotalBytes'] = grandparent._properties['record_bytes'] if grandparent and grandparent._properties.has_key('record_bytes') else -1

            if c._table:
                ordered_dict.update(TableMetadata.encode(c._table))
            else:
                print 'Null Table for record: ', c._name_full, c._is_local_top, c._is_global_top, c._is_dummy

            return ordered_dict
        else:
            raise TypeError('Object of type {} is not JSON serializable'.format(c.__class__.__name__))
