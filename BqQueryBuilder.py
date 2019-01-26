

class BqQueryBuilder(object):

    def __init__(self, table_name, field_schema, field_name):
        self._table_name = table_name
        self._field_schema = field_schema
        self._field_name = field_name
        self._schema_stack = self.buildStack()


    def buildColumnSizeQuery(self):
        # build stack
        query = "SELECT {} FROM `" + self._table_name + "` "

        requested_column_alias = ""
        parent = None
        while self._schema_stack:
            curr_column = self._schema_stack.pop()
            if (not parent or parent._mode != 'REPEATED'):
                requested_column_alias += "." + curr_column._name_short if requested_column_alias != "" else curr_column._name_short
            else: # REPEATED
                #query += "\nCROSS JOIN UNNEST("+ curr_column._name_full +") AS " + curr_column._name_short #or alias
                query += "\nCROSS JOIN UNNEST(" + requested_column_alias + ") AS " + parent._name_short  # or alias
                requested_column_alias = parent._name_short + "." + curr_column._name_short

            parent = curr_column

        #   continue
        # else:
        #  1. ADD: CROSS JOIN UNNEST(current_column_full_field_name) AS current_column_alias
        #  2. replace requested_column_alias prefix of current_column_full_name with current_column_full_field_name+.+all the rest:

        return query.format(requested_column_alias)

    def buildColumnSizeQuery_non_optimized(self):
        # build stack
        query = "SELECT {} FROM `" + self._table_name + "` "

        requested_column_alias = ""
        while self._schema_stack:
            curr_column = self._schema_stack.pop()
            if (curr_column._mode != 'REPEATED'):
                requested_column_alias += "." + curr_column._name_short if requested_column_alias != "" else curr_column._name_short
            else: # REPEATED
                #query += "\nCROSS JOIN UNNEST("+ curr_column._name_full +") AS " + curr_column._name_short #or alias
                query += "\nCROSS JOIN UNNEST(" + requested_column_alias + "." + curr_column._name_short + ") AS " + curr_column._name_short  # or alias
                requested_column_alias = curr_column._name_short

        #   continue
        # else:
        #  1. ADD: CROSS JOIN UNNEST(current_column_full_field_name) AS current_column_alias
        #  2. replace requested_column_alias prefix of current_column_full_name with current_column_full_field_name+.+all the rest:

        return query.format(requested_column_alias)

    def buildStack(self):
        stack = []
        curr_field = self._field_schema
        while curr_field is not None:
            stack.append(curr_field)
            curr_field = curr_field._parent

        return stack



