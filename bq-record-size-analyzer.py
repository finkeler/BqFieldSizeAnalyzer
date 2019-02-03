import argparse
import datetime
import json
import BqCliDriver

from BqQueryBuilder import BqQueryBuilder
from RecordMetadata import RecordMetadata

from JsonSchemaParser import JsonSchemaParser
from TableMetadata import TableMetadata


def main():

    args = parse_args()
    dates = create_dates_range(args)
    for date in dates:
        ## Get Table metadata (byte size, num rows)
        table_name = args.table_prefix + '_' + date.strftime('%Y%m%d')
        table_metadata = create_table_metadata(args.project, args.dataset,table_name)

        ## parse json schema
        schema_parser = JsonSchemaParser(table_metadata['schema']['fields'])
        schema_parser.parse()

        # filter records by criteria
        records_metadata = create_requested_records_metadata(args, schema_parser)

        builder = BqQueryBuilder(args.project, args.dataset, table_name)
        builder.build_record_size_queries(records_metadata)

        for cm in records_metadata:
             cm.set_date(date.strftime('%Y-%m-%d'))
             update_record_total_bytes(cm)
             update_table_metadata(args, table_name, cm)
        #     #update elements length

        print records_metadata
        output_id = args.command.replace('-', '_') + '_' + date.strftime('%Y%m%d')
        out_table_json_file_name = output_id + '.json'
        outfile = open(args.out_json_folder + out_table_json_file_name, 'w')

        # format output
        result = [json.dumps(record, default=RecordMetadata.encode) for record in records_metadata]  # the only significant line to convert the JSON to the desired format
        newline_del_str = '\n'.join(result)
        outfile.write(newline_del_str)
        outfile.close()

        ## load to BQ
        #out_project='spd-test-169914'
        #out_dataset = 'eranf'
        #out_table_name = output_id + date.strftime('%Y%m%d')
        out = BqCliDriver.load_table(args.out_project, args.out_dataset, output_id, '/Users/eran.f/work/python/json/' + out_table_json_file_name, True, 'date')
        print out




def parse_args():

    parent_parser_1 = argparse.ArgumentParser(add_help=False)
    parent_parser_1.add_argument('--from-date', type=str, dest='from_date', help='First day of table. default: yesterday')
    parent_parser_1.add_argument('--to-date', type=str, dest='to_date', help='Last day of table. default: from_date')
    parent_parser_1.add_argument('-p', '--in-project', type=str, dest='project', default='taboola-data', help='default: taboola-data')
    parent_parser_1.add_argument('-s', '--in-dataset', type=str, dest='dataset', default='pageviews', help='default: pageviews')
    parent_parser_1.add_argument('-n', '--in-daily-table-prefix', type=str, dest='table_prefix', default='pageviews', help='daily table prefix name. default: pageviews')
    parent_parser_1.add_argument('-d', '--dry-run', dest='dry_run', default='store_true', help='dry-run mode')
    #parent_parser_1.add_argument('-l', '--log-file', type=str,  dest='logFile', default=DEFAULT_LOG_FILE, help='Log file name')
    parent_parser_1.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Enable debug logging')
    parent_parser_1.add_argument('-j', '--out-json-folder', dest='out_json_folder', help='outut json files folder', default='/Users/eran.f/work/python/json/') # TODO: change default
    parent_parser_1.add_argument('--out-dataset', required=True, dest='out_dataset', help='output dataset')
    parent_parser_1.add_argument('--out-project', required=True, dest='out_project', help='output project')


    parent_parser_2 = argparse.ArgumentParser(add_help=False)
    parent_parser_2.add_argument('-r', '--record-name', type=str, dest='record_name', help='') # TODO: NEED TO SUPPORT A LIST

    parser = argparse.ArgumentParser(description='bq-table-metadata-analyzer')
    subparsers = parser.add_subparsers(dest="command", title='sub-commands',help='sub-commands help')
    subparsers.add_parser('full-scan', parents=[parent_parser_1], help='full-scan help')
    subparsers.add_parser('record-name-scan', parents=[parent_parser_1, parent_parser_2], help='record-name-scan help')
    subparsers.add_parser('record-name-deep-scan', parents=[parent_parser_1, parent_parser_2], help='record-name-deep-scan help')
    level_parser = subparsers.add_parser('record-level-scan', parents=[parent_parser_1], help='record-level-scan help')
    level_parser.add_argument('-l', '--level', required=True, type=int, dest='level', default='', help='nesting level from 0. default: 0')

    type_parser = subparsers.add_parser('record-type-scan', parents=[parent_parser_1], help='record-type-scan help')
    type_parser.add_argument('-y', '--type', required=True, type=str, choices=['BOOLEAN', 'FLOAT', 'INTEGER', 'RECORD', 'STRING', 'TIMESTAMP'], dest='record_type', help='record type. see: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types')

    mode_parser = subparsers.add_parser('record-mode-scan', parents=[parent_parser_1], help='record-mode-scan help')
    mode_parser.add_argument('-m', '--mode', required=True, type=str, choices=['NULLABLE', 'REQUIRED', 'REPEATED'], dest='record_mode', help='record mode. see: https://cloud.google.com/bigquery/docs/schemas#modes')

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

def update_record_total_bytes(record_metadata):

    dry_run_output_json = BqCliDriver.execute_query(record_metadata._queries['recordSizeQuery'], True)
    try:
        dry_run_output = json.loads(dry_run_output_json)
    except Exception as e:
        raise Exception('Unable to decode json. reason: {0}, json: {1}'.format(e.message, dry_run_output_json))


    query_stats = dry_run_output['statistics']['query']
    total_bytes = int(query_stats['totalBytesProcessed'])
    total_bytes_accuracy = query_stats['totalBytesProcessedAccuracy']
    print record_metadata._schema._name_full, total_bytes, total_bytes_accuracy
    record_metadata.add_property('record_bytes', total_bytes)
    record_metadata.add_property('record_bytes_accuracy', total_bytes_accuracy)

def update_table_metadata(args, table_name, record_metadata):
    table_metadata_dict = create_table_metadata(args.project, args.dataset,table_name)

    parsed_table_metadata = TableMetadata(str(table_metadata_dict['id']),
                                          datetime.datetime.fromtimestamp(int(table_metadata_dict['creationTime'])/1000.0),
                                          datetime.datetime.fromtimestamp(int(table_metadata_dict['lastModifiedTime'])/1000.0),
                                          int(table_metadata_dict['numRows']),
                                          int(table_metadata_dict['numBytes']))
    record_metadata.set_table(parsed_table_metadata)

def create_requested_records_metadata(args, schema_parser):
    command = args.command

    # filter record by criteria
    if command == 'full-scan':
        records_schema = schema_parser.field_name_dictionary.itervalues()
    elif command == 'record-name-scan':
        records_schema = [schema_parser.field_name_dictionary[args.record_name]]
    elif command == 'record-name-deep-scan':
        records_schema = []
        for key, value in schema_parser.field_name_dictionary.iteritems():
            if key.startswith(args.record_name):
                records_schema.append(value)
    elif command == 'record-level-scan':
        pass
    elif command == 'record-type-scan':
        records_schema = schema_parser._field_type_dictionary[args.record_type]
    elif command == 'record-mode-scan':
        records_schema = schema_parser._field_mode_dictionary[args.record_mode]

    records_metadata = map(lambda c: RecordMetadata(c), records_schema)
    return records_metadata



if __name__ == '__main__':
    main()