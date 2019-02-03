import datetime
from collections import OrderedDict


class TableMetadata(object):

    def __init__(self, table_name, creation_time, last_modified_time, num_rows, num_bytes):
        self._name=table_name
        self._creation_time=creation_time
        self._last_modified_time=last_modified_time
        self._num_rows=num_rows
        self._num_bytes=num_bytes
        #self._schema = schema
        #self.properties = {}

    def set_last_modified_time(self, last_modified_time):
        self._last_modified_time = last_modified_time

    def set_num_bytes(self, num_bytes):
        self._num_bytes = num_bytes

    def set_num_rows(self, num_rows):
        self._num_rows = num_rows

    def encode(t):
        if isinstance(t, TableMetadata):
            orderedDict = OrderedDict()
            orderedDict['tableName']= t._name
            orderedDict['tableCreationTime']= t._creation_time.strftime('%Y-%m-%d %H:%M:%S')
            orderedDict['tableLastModifiedTime']= t._last_modified_time.strftime('%Y-%m-%d %H:%M:%S')
            orderedDict['tableRows'] = t._num_rows
            orderedDict['tableNumBytes'] = t._num_bytes

            return orderedDict
        else:
            raise TypeError('Object of type {} is not JSON serializable'.format(t.__class__.__name__))