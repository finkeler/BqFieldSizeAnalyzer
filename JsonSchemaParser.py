import json
from collections import defaultdict

from FieldSchema import FieldSchema
from TableSchema import TableSchema


class JsonSchemaParser(object):

    def __init__(self, json_file_name):
        self._file_name = json_file_name #raw_schema is a list of dictionaries
        self._json_file = open(json_file_name)
        self._column_name_dictionary = {}
        self._column_level_dictionary = defaultdict(list)
        self._column_type_dictionary = defaultdict(list)
        self._column_mode_dictionary = defaultdict(list)


    def parse(self):
        """Deserialize json to raw python object hold a list of dictionaries"""
        raw_json_data = json.loads(self._json_file.read())

        return self.parse_raw_data(raw_json_data)


    def parse_raw_data(self, raw_json_data):
        #run over the list and create a tuple? list
        columns = [self.create_schema_columns(d) for d in raw_json_data]
        return TableSchema(columns) #maybe need tuple()

    # #@classmethod
    # def create_schema_columns_rec(self, column, __parent=None, level=0):
    #     # Optional properties with default values
    #     mode = column.get('mode', 'NULLABLE')
    #     description = column.get('description')
    #     fields = column.get('fields', ())
    #     name = column['name']
    #     name_full = __parent + '.' + name if __parent is not None else name
    #
    #     return FieldSchema(
    #         name_short=name,
    #         name_full=name_full,
    #         field_type=column['type'].upper(),
    #         level=level,
    #         mode=mode.upper(),
    #         description=description,
    #         parent=__parent, # parent_full
    #         fields = [self.create_schema_columns_rec(f, name_full, level + 1) for f in fields]
    #     )


    def create_schema_columns(self, column, parent=None, level=0):

        # Optional properties with default values
        mode = column.get('mode', 'NULLABLE').upper()
        description = column.get('description')
        fields = column.get('fields', ())
        name_short = column['name']
        name_full = parent._name_full + '.' + name_short if parent is not None else name_short

        if parent and mode != 'REPEATED':
            alias = parent._alias + '.' + name_short
        else:
            alias = name_short

        currFieldSchema = FieldSchema(
            name_short=name_short,
            name_full=name_full,
            field_type=column['type'].upper(),
            level=level,
            mode=mode, #.upper(),
            description=description,
            parent=parent,
            alias=alias
        )

        self.create_catalog(currFieldSchema)
        currFieldSchema._fields = [self.create_schema_columns(f, currFieldSchema, level + 1) for f in fields]
        return currFieldSchema

    def create_catalog(self, fieldSchema):
        self._column_name_dictionary[fieldSchema._name_full] = fieldSchema
        self._column_level_dictionary[fieldSchema._level].append(fieldSchema)
        self._column_type_dictionary[fieldSchema._field_type].append(fieldSchema)
        self._column_mode_dictionary[fieldSchema._mode].append(fieldSchema)

    @property
    def column_name_dictionary(self):
        return self._column_name_dictionary


