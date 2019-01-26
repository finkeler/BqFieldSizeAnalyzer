

class BqQueryBuilder(object):

    def __init__(self, table_name):
        self._table_name = table_name

    def buildColumnSizeQueries(self, column_list):
        for column in column_list:
            query = self.buildColumnSizeQuery(column._schema)
            column.addQuery('columnSizeQuery', query)


    def buildColumnSizeQuery(self, column):
        query = "SELECT {} FROM `" + self._table_name + "` "
        schema_stack = self.buildStack(column)
        requested_column_alias = ""
        parent = None

        while schema_stack:
            curr_column = schema_stack.pop()
            if (parent and parent._mode == 'REPEATED'):
                query += "\nCROSS JOIN UNNEST(" + requested_column_alias + ") AS " + parent._name_short  # or alias
                requested_column_alias = parent._name_short + "." + curr_column._name_short
            else:
                requested_column_alias += "." + curr_column._name_short if requested_column_alias != "" else curr_column._name_short

            parent = curr_column

        return query.format(requested_column_alias)


    def buildStack(self, column):
        stack = []
        curr_column = column
        while curr_column is not None:
            stack.append(curr_column)
            curr_column = curr_column._parent

        return stack



