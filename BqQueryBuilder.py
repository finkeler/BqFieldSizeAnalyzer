

class BqQueryBuilder(object):

    def __init__(self, project, dataset, table_name):
        self._table_name = '.'.join([project, dataset, table_name])
        print self._table_name

    def build_record_size_queries(self, records_list):
        for record in records_list:
            query = self.build_record_size_query(record)
            record.add_query('recordSizeQuery', query)


    def build_record_size_query(self, record):
        query = "SELECT {} FROM `" + self._table_name + "` "
        record_stack = self.build_stack(record)
        requested_record_alias = ""
        parent = None

        while record_stack:
            curr_record = record_stack.pop()
            if (parent and parent._mode == 'REPEATED'):
                query += " CROSS JOIN UNNEST(" + requested_record_alias + ") AS " + parent._name_short  # or alias
                requested_record_alias = parent._name_short + "." + curr_record._name_short
            else:
                requested_record_alias += "." + curr_record._name_short if requested_record_alias != "" else curr_record._name_short

            parent = curr_record

        return query.format(requested_record_alias)


    def build_stack(self, record):
        stack = []
        curr_record = record
        while curr_record is not None:
            stack.append(curr_record)
            curr_record = curr_record._parent

        return stack



