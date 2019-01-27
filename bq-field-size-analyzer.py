import argparse
import datetime
import json
import subprocess

from BqExecuter import BqExecuter
from BqQueryBuilder import BqQueryBuilder
from ColumnMetadata import ColumnMetadata

from JsonSchemaParser import JsonSchemaParser


def main():


    # parse input
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
        command = ['bq', 'show', '--format=json', table_name]
        output = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE).communicate()[0]
        table_metadata = json.loads(output)

        ## parse json Schema

        schemaParser = JsonSchemaParser('pageview-simple-json-schema.json')
        schema = schemaParser.parse()

        ## prepare column list based on cmd line args and pass them to a class to:
        ## this class creates queries, execute and builds output dictionary(?)

        # analyze a single column name (fully qualified) with childs
        # for key, value in schemaParser.column_name_dictionary.iteritems():
        #     if key.startswith('pv.requests.servedItems'):
        #         pass

        # prepare list of columns
        columns = map(lambda (name, col): ColumnMetadata(name, col), schemaParser.column_name_dictionary.iteritems())

        builder = BqQueryBuilder('taboola-data.pageviews.pageviews_20190120')
        builder.buildColumnSizeQueries(columns)

        bqExecuter = BqExecuter()
        for column in columns:
            size = bqExecuter.execute(column._queries['columnSizeQuery'])
            column.addProperty('size_bytes', size)

    # schemaField = schemaParser.column_name_dictionary['pv.requests.postAuctionEvent.postAuctionData.auctionBidData.lost.bid']
    #
    # query = builder.buildColumnSizeQuery(schemaField)
    # print query
    #
    # print '\n'
    #
    # schemaField = schemaParser.column_name_dictionary['pv.requests.postAuctionEvent']
    # query2 = builder.buildColumnSizeQuery(schemaField)
    # print query2
    #
    # print '\n'
    # schemaField = schemaParser.column_name_dictionary['pv.userPageviewCounters.counterNames']
    # query3 = builder.buildColumnSizeQuery(schemaField)
    # print query3
    #
    # print '\n'
    #
    # schemaField = schemaParser.column_name_dictionary['pv.userSegmentsInfo.uddsAgeDays.uddId']
    # query4 = builder.buildColumnSizeQuery(schemaField)
    # print query4
    #
    # print '\n'
    #
    # schemaField = schemaParser.column_name_dictionary['pv.requests']
    # query2 = builder.buildColumnSizeQuery(schemaField)
    # print query2
    #
    #
    # print '\n'
    #
    # schemaField = schemaParser.column_name_dictionary['sessionStartTimeTimeSlice']
    # query2 = builder.buildColumnSizeQuery(schemaField)
    # print query2




# Schema parsing
## get table schema via bq as json file
## read json as str (via json_file.read() )
## parse json (via json.loads(str) ) to get a list of unformatted dictionaries

## need to hold a dictionary of <full_column_name>:<SchemaFieldObject>
## SchemaFieldObject should hold its own data (including size, level) and also a ref to his parent. this is usefull when we need to traverse the schema to find what we needed to explode in queries

# Need a class that takes a table object and an input instruction such as :
## a. analyze a single column name (fully qualified) without childs
## b. analyze a single column name (fully qualified) with childs
## c. traverse all columns
## d. traverse by level
## e. traverse by type / and specifically - lists/strings
## f. traverse by mode (repeated)

## the class creates instructions based on the schema
## the class execute the commands (or just print them)
## the class builds an output from the table & executed commands (per field: name, parent, field size, etc..
## format output
## load to BQ


def parseArgs():

    parent_parser_1 = argparse.ArgumentParser(add_help=False)
    parent_parser_1.add_argument('-f', '--from-date', type=str, dest='from_date', help='First day of table. default: yesterday')
    parent_parser_1.add_argument('-t', '--to-date', type=str, dest='to_date', help='Last day of table. default: from_date')
    parent_parser_1.add_argument('-p', '--project', type=str, dest='project', default='taboola-data', help='default: taboola-data')
    parent_parser_1.add_argument('-d', '--dataset', type=str, dest='dataset', default='pageviews', help='default: pageviews')
    parent_parser_1.add_argument('-n', '--daily-table-prefix', type=str, dest='table_prefix', default='pageviews', help='daily table prefix name. default: pageviews')

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
    type_parser.add_argument('-y', '--type', required=True, type=str, dest='column_type', help='column type. see: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types')

    mode_parser = subparsers.add_parser('column-mode-scan', parents=[parent_parser_1], help='column-mode-scan help')
    mode_parser.add_argument('-m', '--mode', required=True, type=str, choices=['NULLABLE', 'REQUIRED', 'REPEATED'], dest='column_mode', help='column mode. see: https://cloud.google.com/bigquery/docs/schemas#modes')

    subparsers.add_parser('lists-scan', parents=[parent_parser_1], help='lists-scan help')

    args = parser.parse_args()
    print args
    return args

if __name__ == '__main__':
    main()