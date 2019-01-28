"""Schemas for BigQuery tables"""

class FieldSchema(object):

    def __init__(self, name_short, name_full, field_type, level, parent=None, alias=None,mode='NULLABLE',
                 description=None, fields=(), is_leaf=True):
        self._name_short = name_short #can be optimized? via name_full
        self._name_full = name_full
        self._parent = parent
        self._level = level
        self._field_type = field_type
        self._mode = mode
        self._description = description
        self._fields = tuple(fields)
        self._is_leaf = is_leaf


    @property
    def name_short(self):
        """str: The name of the field."""
        return self._name_short

    @property
    def field_type(self):
        """str: The type of the field.
        Will be one of 'STRING', 'INTEGER', 'FLOAT', 'NUMERIC',
        'BOOLEAN', 'TIMESTAMP' or 'RECORD'.
        """
        return self._field_type

    @property
    def mode(self):
        """str: The mode of the field.
        Will be one of 'NULLABLE', 'REQUIRED', or 'REPEATED'.
        """
        return self._mode

    @property
    def is_nullable(self):
        """Check whether 'mode' is 'nullable'."""
        return self._mode == 'NULLABLE'

    @property
    def description(self):
        """Optional[str]: Description for the field."""
        return self._description

    @property
    def fields(self):
        """tuple: Subfields contained in this field.
        If ``field_type`` is not 'RECORD', this property must be
        empty / unset.
        """
        return self._fields

    def _key(self):
        """A tuple key that uniquely describes this field.
        Used to compute this instance's hashcode and evaluate equality.
        Returns:
            tuple: The contents of this
                   :class:`~google.cloud.bigquery.schema.SchemaField`.
        """
        return (
            self._name_short,
            self._name_full,
            self._field_type.upper(),
            self._mode.upper(),
            self._description,
            self._fields,
            self._level,
            self._is_leaf
        )

    def __eq__(self, other):
        if not isinstance(other, FieldSchema):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'SchemaField{}'.format(self._key())


