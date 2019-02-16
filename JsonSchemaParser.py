import json
import logging
from collections import defaultdict

from FieldSchema import FieldSchema
from RecordMetadata import RecordMetadata
from TableSchema import TableSchema


class JsonSchemaParser(object):

    def __init__(self, json_schema):
        self._json_schema = json_schema #raw_schema is a list of dictionaries
        self._field_name_dictionary = {}
        self._leaves_dictionary = {}
        self._field_level_dictionary = defaultdict(list)
        self._field_type_dictionary = defaultdict(list)
        self._field_mode_dictionary = defaultdict(list)
        self._parent_dictionary = defaultdict(list)


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

        curr_record = RecordMetadata(
            name_short=name_short,
            name_full=name_full,
            field_type=field['type'].upper(),
            level=level,
            mode=mode,
            description=description,
            parent=parent,
            is_leaf=is_leaf
        )

        self.update_catalog(curr_record)
        curr_record._fields = [self.create_schema(child_field, curr_record, level + 1) for child_field in child_fields]
        return curr_record

    def update_catalog(self, record_metadata):
        self._field_name_dictionary[record_metadata._name_full] = record_metadata
        self._field_level_dictionary[record_metadata._level].append(record_metadata)
        self._field_type_dictionary[record_metadata._field_type].append(record_metadata)
        self._field_mode_dictionary[record_metadata._mode].append(record_metadata)
        self._parent_dictionary[record_metadata._parent._name_full if record_metadata._parent else None].append(record_metadata)

        if record_metadata._is_leaf:
            self._leaves_dictionary[record_metadata._name_full] = record_metadata

    @property
    def field_name_dictionary(self):
        return self._field_name_dictionary

    @property
    def parent_dictionary(self):
        return self._parent_dictionary


