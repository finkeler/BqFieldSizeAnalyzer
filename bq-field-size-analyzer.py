import argparse
import datetime
import json
import BqCliDriver

from BqQueryBuilder import BqQueryBuilder
from ColumnMetadata import ColumnMetadata

from JsonSchemaParser import JsonSchemaParser
from TableMetadata import TableMetadata


def main():

    args = parseArgs()
    dates = create_dates_range(args)
    for date in dates:
        ## Get Table metadata (byte size, num rows)
        table_name = args.table_prefix + '_' + date.strftime('%Y%m%d')
        table_metadata = create_table_metadata(args.project, args.dataset,table_name)

        ## parse json schema
        schema_parser = JsonSchemaParser(table_metadata['schema']['fields'])
        schema_parser.parse()

        # filter columns by criteria
        columns_metadata = create_requested_columns_metadata(args, schema_parser)

        builder = BqQueryBuilder(args.project, args.dataset, table_name)
        builder.buildColumnSizeQueries(columns_metadata)

        for cm in columns_metadata:
             cm.set_date(date.strftime('%Y-%m-%d'))
             update_column_total_bytes(cm)
             update_table_metadata(args, table_name, cm)
        #     #update elements length

        print columns_metadata
        out_table_json_file_name = 'data_' + date.strftime('%Y%m%d') + '.json'
        outfile = open('/Users/eran.f/work/python/json/' + out_table_json_file_name, 'w')

        # format output
        result = [json.dumps(record, default=ColumnMetadata.encode) for record in columns_metadata]  # the only significant line to convert the JSON to the desired format
        newline_del_str = '\n'.join(result)
        outfile.write(newline_del_str)
        outfile.close()

        ## load to BQ
        out_project='spd-test-169914'
        out_dataset = 'eranf'
        out_table_name = 'repeated_records_table_' + date.strftime('%Y%m%d')
        out = BqCliDriver.load_table(out_project, out_dataset, out_table_name, '/Users/eran.f/work/python/json/' + out_table_json_file_name, True, 'date')
        print out




def parseArgs():

    parent_parser_1 = argparse.ArgumentParser(add_help=False)
    parent_parser_1.add_argument('-f', '--from-date', type=str, dest='from_date', help='First day of table. default: yesterday')
    parent_parser_1.add_argument('-t', '--to-date', type=str, dest='to_date', help='Last day of table. default: from_date')
    parent_parser_1.add_argument('-p', '--project', type=str, dest='project', default='taboola-data', help='default: taboola-data')
    parent_parser_1.add_argument('-s', '--dataset', type=str, dest='dataset', default='pageviews', help='default: pageviews')
    parent_parser_1.add_argument('-n', '--daily-table-prefix', type=str, dest='table_prefix', default='pageviews', help='daily table prefix name. default: pageviews')
    parent_parser_1.add_argument('-d', '--dry-run', dest='dry_run', default='store_true', help='dry-run mode')
    #parent_parser_1.add_argument('-l', '--log-file', type=str,  dest='logFile', default=DEFAULT_LOG_FILE, help='Log file name')
    parent_parser_1.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Enable debug logging')

    parent_parser_2 = argparse.ArgumentParser(add_help=False)
    parent_parser_2.add_argument('-c', '--column-name', type=str, dest='column_name', help='') # TODO: NEED TO SUPPORT A LIST

    parser = argparse.ArgumentParser(description='bq-table-metadata-analyzer')
    subparsers = parser.add_subparsers(dest="command", title='sub-commands',help='sub-commands help')
    subparsers.add_parser('full-scan', parents=[parent_parser_1], help='full-scan help')
    subparsers.add_parser('column-name-scan', parents=[parent_parser_1, parent_parser_2], help='column-name-scan help')
    subparsers.add_parser('column-name-deep-scan', parents=[parent_parser_1, parent_parser_2], help='column-name-deep-scan help')
    level_parser = subparsers.add_parser('column-level-scan', parents=[parent_parser_1], help='column-level-scan help')
    level_parser.add_argument('-l', '--level', required=True, type=int, dest='level', default='', help='nesting level from 0. default: 0')

    type_parser = subparsers.add_parser('column-type-scan', parents=[parent_parser_1], help='column-type-scan help')
    type_parser.add_argument('-y', '--type', required=True, type=str, choices=['BOOLEAN', 'FLOAT', 'INTEGER', 'RECORD', 'STRING', 'TIMESTAMP'], dest='column_type', help='column type. see: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types')

    mode_parser = subparsers.add_parser('column-mode-scan', parents=[parent_parser_1], help='column-mode-scan help')
    mode_parser.add_argument('-m', '--mode', required=True, type=str, choices=['NULLABLE', 'REQUIRED', 'REPEATED'], dest='column_mode', help='column mode. see: https://cloud.google.com/bigquery/docs/schemas#modes')

    subparsers.add_parser('lists-scan', parents=[parent_parser_1], help='lists-scan help')

    level_parser = subparsers.add_parser('top-records-scan', parents=[parent_parser_1], help='top-records-scan help')
    level_parser.add_argument('-a', '--as-of-date', required=True, type=str, dest='top_records_as_of', default='', help='reference date to query top records')

    args = parser.parse_args()
    print args
    return args

def create_dates_range(args):
    if not args.from_date:
        today = datetime.date.today()
        from_date = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
        print from_date
    else:
        from_date = datetime.datetime.strptime(args.from_date, '%Y%m%d')

    if not args.to_date:
        to_date = from_date
        print to_date
    else:
        to_date = datetime.datetime.strptime(args.to_date, '%Y%m%d')

    date_list = []
    while from_date <= to_date:
        date_list.append(from_date)
        from_date = from_date + datetime.timedelta(days=1)

    return date_list

def create_table_metadata(project, dataset, table_name):
    table_metadata_json = BqCliDriver.show_table(project, dataset, table_name, 'json')
    try:
        metadata = json.loads(table_metadata_json)
    except Exception as e:
        raise Exception('Unable to decode json. reason: {0}, json: {1}'.format(e.message, table_metadata_json))

    return metadata
    #build TableMetadata object

def update_column_total_bytes(column_metadata):

    dry_run_output_json = BqCliDriver.execute_query(column_metadata._queries['columnSizeQuery'], True)
    try:
        dry_run_output = json.loads(dry_run_output_json)
    except Exception as e:
        raise Exception('Unable to decode json. reason: {0}, json: {1}'.format(e.message, dry_run_output_json))


    query_stats = dry_run_output['statistics']['query']
    total_bytes = int(query_stats['totalBytesProcessed'])
    total_bytes_accuracy = query_stats['totalBytesProcessedAccuracy']
    print column_metadata._schema._name_full, total_bytes, total_bytes_accuracy
    column_metadata.addProperty('record_bytes', total_bytes)
    column_metadata.addProperty('record_bytes_accuracy', total_bytes_accuracy)

def update_table_metadata(args, table_name, column_metadata):
    table_metadata_dict = create_table_metadata(args.project, args.dataset,table_name)

    parsed_table_metadata = TableMetadata(str(table_metadata_dict['id']),
                                          datetime.datetime.fromtimestamp(int(table_metadata_dict['creationTime'])/1000.0),
                                          datetime.datetime.fromtimestamp(int(table_metadata_dict['lastModifiedTime'])/1000.0),
                                          int(table_metadata_dict['numRows']),
                                          int(table_metadata_dict['numBytes']))
    column_metadata.set_table(parsed_table_metadata)

def create_requested_columns_metadata(args, schemaParser):
    command = args.command

    # filter columns by criteria
    if command == 'full-scan':
        columns = schemaParser.column_name_dictionary.itervalues()
    elif command == 'column-name-scan':
        columns = [schemaParser.column_name_dictionary[args.column_name]]
    elif command == 'column-name-deep-scan':
        columns = []
        for key, value in schemaParser.column_name_dictionary.iteritems():
            if key.startswith(args.column_name):
                columns.append(value)
    elif command == 'column-level-scan':
        pass
    elif command == 'column-type-scan':
        columns = schemaParser._column_type_dictionary[args.column_type]
    elif command == 'column-mode-scan':
        columns = schemaParser._column_mode_dictionary[args.column_mode]

    columnsMetadata = map(lambda c: ColumnMetadata(c), columns)
    return columnsMetadata



if __name__ == '__main__':
    main()