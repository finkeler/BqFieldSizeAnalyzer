import json
from collections import defaultdict

from FieldSchema import FieldSchema
from TableSchema import TableSchema


class JsonSchemaParser(object):

    def __init__(self, json_schema):
        self._json_schema = json_schema #raw_schema is a list of dictionaries
        self._field_name_dictionary = {}
        self._leaves_dictionary = {}
        self._field_level_dictionary = defaultdict(list)
        self._field_type_dictionary = defaultdict(list)
        self._field_mode_dictionary = defaultdict(list)


    def parse(self):
        #run over the list and create a tuple? list
        columns = [self.create_schema(field) for field in self._json_schema]
        return TableSchema(columns) #maybe need tuple()

    def create_schema(self, field, parent=None, level=0):

        # Optional properties with default values
        mode = field.get('mode', 'NULLABLE').upper().encode("utf-8")
        description = field.get('description')
        child_fields = field.get('fields', ())
        is_leaf = len(child_fields) == 0
        name_short = field['name'].encode("utf-8")
        name_full = parent._name_full + '.' + name_short if parent is not None else name_short

        curr_field_schema = FieldSchema(
            name_short=name_short,
            name_full=name_full,
            field_type=field['type'].upper(),
            level=level,
            mode=mode,
            description=description,
            parent=parent,
            is_leaf=is_leaf
        )

        self.update_catalog(curr_field_schema)
        curr_field_schema._fields = [self.create_schema(child_field, curr_field_schema, level + 1) for child_field in child_fields]
        return curr_field_schema

    def update_catalog(self, field_schema):
        self._field_name_dictionary[field_schema._name_full] = field_schema
        self._field_level_dictionary[field_schema._level].append(field_schema)
        self._field_type_dictionary[field_schema._field_type].append(field_schema)
        self._field_mode_dictionary[field_schema._mode].append(field_schema)

        if field_schema._is_leaf:
            self._leaves_dictionary[field_schema._name_full] = field_schema

    @property
    def field_name_dictionary(self):
        return self._field_name_dictionary


