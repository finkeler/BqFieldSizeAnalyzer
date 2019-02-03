

class BqQueryBuilder(object):

    def __init__(self, project, dataset, table_name):
        self._table_name = '.'.join([project, dataset, table_name])
        print self._table_name

    def build_record_size_queries(self, records_list):
        for record in records_list:
            query = self.build_record_size_query(record._schema)
            record.add_query('recordSizeQuery', query)


    def build_record_size_query(self, record_schema):
        query = "SELECT {} FROM `" + self._table_name + "` "
        schema_stack = self.build_stack(record_schema)
        requested_record_alias = ""
        parent = None

        while schema_stack:
            curr_record_schema = schema_stack.pop()
            if (parent and parent._mode == 'REPEATED'):
                query += " CROSS JOIN UNNEST(" + requested_record_alias + ") AS " + parent._name_short  # or alias
                requested_record_alias = parent._name_short + "." + curr_record_schema._name_short
            else:
                requested_record_alias += "." + curr_record_schema._name_short if requested_record_alias != "" else curr_record_schema._name_short

            parent = curr_record_schema

        return query.format(requested_record_alias)


    def build_stack(self, record_schema):
        stack = []
        curr_record_schema = record_schema
        while curr_record_schema is not None:
            stack.append(curr_record_schema)
            curr_record_schema = curr_record_schema._parent

        return stack



