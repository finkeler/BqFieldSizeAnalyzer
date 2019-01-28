import json
from collections import defaultdict

from FieldSchema import FieldSchema
from TableSchema import TableSchema


class JsonSchemaParser(object):

    def __init__(self, json_schema):
        self._json_schema = json_schema #raw_schema is a list of dictionaries
        self._column_name_dictionary = {}
        self._column_leaves_dictionary = {}
        self._column_level_dictionary = defaultdict(list)
        self._column_type_dictionary = defaultdict(list)
        self._column_mode_dictionary = defaultdict(list)


    def parse(self):
        #run over the list and create a tuple? list
        columns = [self.create_schema_columns(d) for d in self._json_schema]
        return TableSchema(columns) #maybe need tuple()

    def create_schema_columns(self, column, parent=None, level=0):

        # Optional properties with default values
        mode = column.get('mode', 'NULLABLE').upper()
        description = column.get('description')
        fields = column.get('fields', ())
        is_leaf = len(fields) == 0
        name_short = column['name']
        name_full = parent._name_full + '.' + name_short if parent is not None else name_short

        currFieldSchema = FieldSchema(
            name_short=name_short,
            name_full=name_full,
            field_type=column['type'].upper(),
            level=level,
            mode=mode,
            description=description,
            parent=parent,
            is_leaf=is_leaf
        )

        self.update_catalog(currFieldSchema)
        currFieldSchema._fields = [self.create_schema_columns(f, currFieldSchema, level + 1) for f in fields]
        return currFieldSchema

    def update_catalog(self, fieldSchema):
        self._column_name_dictionary[fieldSchema._name_full] = fieldSchema
        self._column_level_dictionary[fieldSchema._level].append(fieldSchema)
        self._column_type_dictionary[fieldSchema._field_type].append(fieldSchema)
        self._column_mode_dictionary[fieldSchema._mode].append(fieldSchema)

        if fieldSchema._is_leaf:
            self._column_leaves_dictionary[fieldSchema._name_full] = fieldSchema

    @property
    def column_name_dictionary(self):
        return self._column_name_dictionary


