import argparse
import datetime
import json
import subprocess

from BqExecuter import BqExecuter
from BqQueryBuilder import BqQueryBuilder
from ColumnMetadata import ColumnMetadata

from JsonSchemaParser import JsonSchemaParser


def main():

    args = parseArgs()
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
        date_list.append(from_date.strftime('%Y%m%d'))
        from_date = from_date + datetime.timedelta(days=1)

    for date in date_list:

        ## convert to table name
        ## Get Table metadata (byte size, num rows)
        ## get table schema via bq as json file
        # bq show --format=json taboola-data:pageviews.pageviews_20190122
        table_name = args.project + ':' + args.dataset + '.' + args.table_prefix + '_' + date
        table_show_command = ['bq', 'show', '--format=json', table_name]
        output = subprocess.Popen(table_show_command, shell=False, stdout=subprocess.PIPE).communicate()[0]
        table_metadata = json.loads(output)

        ## parse json schema
        schemaParser = JsonSchemaParser(table_metadata['schema']['fields'])
        schemaParser.parse()

        # filter columns by criteria
        columnsMetadata = create_columns_metadata(args, schemaParser)

        table_name_for_query = args.project + '.' + args.dataset + '.' + args.table_prefix + '_' + date
        builder = BqQueryBuilder(table_name_for_query)
        builder.buildColumnSizeQueries(columnsMetadata)

        bqExecuter = BqExecuter()
        for cm in columnsMetadata:
            size = bqExecuter.execute(cm._queries['columnSizeQuery'])
            print cm._name_full , size
            cm.addProperty('size_bytes', size)

        print columnsMetadata

        ## builds an output from the columns metadata (per field: name, parent, field size, etc..
        ## format output
        ## load to BQ

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
    parent_parser_2.add_argument('-c', '--column-name', type=str, dest='column_name', help='')

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

    args = parser.parse_args()
    print args
    return args

def create_columns_metadata(args, schemaParser):
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
        pass

    columnsMetadata = map(lambda c: ColumnMetadata(c), columns)
    return columnsMetadata

if __name__ == '__main__':
    main()