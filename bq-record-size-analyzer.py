import argparse
import datetime
import json
import logging
import os
import sys
import time
from dateutil import tz
from functools import partial
from multiprocessing.pool import ThreadPool

import BqCliDriver
from BqQueryBuilder import BqQueryBuilder
from FieldSchema import FieldSchema
from RecordMetadata import RecordMetadata

from JsonSchemaParser import JsonSchemaParser
from TableMetadata import TableMetadata


def main():

    args = parse_args()
    dates = create_dates_range(args)
    logging.basicConfig(filename=args.out_folder + '/logs/' + os.path.basename(os.path.splitext(sys.argv[0])[0]) + '_' + time.strftime("%Y%m%d_%H_%M_%S", time.gmtime()) + '.log',
                        filemode='w', level=logging.INFO, format='%(asctime)s\t%(levelname)s\t%(message)s')

    start_time = datetime.datetime.now().replace(microsecond=0)
    for date in dates:
        ## Get Table metadata (byte size, num rows)
        table_name = args.table_prefix + '_' + date.strftime('%Y%m%d')
        table_metadata = create_table_metadata(args, table_name)

        ## parse json schema
        schema_parser = JsonSchemaParser(table_metadata._json_schema)
        schema_parser.parse()

        # filter records by criteria
        records_metadata = create_requested_records_metadata(args, schema_parser)

        builder = BqQueryBuilder(args.project, args.dataset, table_name)
        builder.build_record_size_queries(records_metadata)

        logging.info("Calculating metadata for %d records", len(records_metadata))

        num_threads = 20
        print 'num threads: ', num_threads
        pool = ThreadPool(processes=num_threads)
        func = partial(apply, date, args, table_name)
        pool.map(func, records_metadata)
        pool.close()
        pool.join()

       # if command == 'full-scan':
        delta_records = create_delta_records(schema_parser)

        # if command == 'full-scan':
        global_others_record = mark_top_records(schema_parser, table_metadata)

        # sort ALL records by their size. Find top 10 without child-duplicates (parent must be larger than a child). blacklist: pv, request, servedItems.
        # Calculate delta and create 11 special records -> top 10 + 1 for delta (table size - top 10 aggr size). mark them as top_10, and top_10 delta ( verify pie support 11 slices, otherwise create 9+1)
        dummy_records = [add_dummy_record(r) for r in records_metadata if not r._is_leaf and not r._is_dummy]


        records_metadata.extend(delta_records)
        if global_others_record:
            records_metadata.append(global_others_record)
        records_metadata.extend(dummy_records)

        print records_metadata

        #sys.exit()

        out_table_prefix = args.out_table if args.out_table else args.command.replace('-', '_')
        output_id = out_table_prefix + '_' + date.strftime('%Y%m%d')
        logging.info("Loading data to BQ table: %s:%s.%s", args.out_project, args.out_dataset, output_id)

        out_table_json_file_name = output_id + '.json'
        outfile = open(args.out_folder + '/json/' + out_table_json_file_name, 'w')


        # format output
        result = [json.dumps(record, default=RecordMetadata.encode) for record in records_metadata]  # the only significant line to convert the JSON to the desired format
        newline_del_str = '\n'.join(result)
        outfile.write(newline_del_str)
        outfile.close()

        ## load to BQ

        out = BqCliDriver.load_table(args.out_project, args.out_dataset, output_id,  args.out_folder + '/json/' + out_table_json_file_name, True, 'date')
        print out

    end_time = datetime.datetime.now().replace(microsecond=0)
    print 'Job duration: ', end_time - start_time

def create_delta_records(schema_parser):

    parent_dictionary = schema_parser.parent_dictionary
    field_name_dictionary = schema_parser.field_name_dictionary
    delta_records = []
    for parent_name, childs in parent_dictionary.iteritems():
        childs.sort(key=lambda ch: ch._properties['record_bytes'] if ch._properties.has_key('record_bytes') else -1, reverse=True)

        if not parent_name:
            continue
        parent = field_name_dictionary[parent_name]
        if not parent._table: # TODO: SHOULD CHECK IF PARENT PROBED (NEED A FLAG). THIS IS JUST A HACK
            print 'parent: ', parent_name, ' is not probed. skipping it'
            continue
        parent_bytes = parent._properties['record_bytes'] if parent._properties.has_key('record_bytes') else -1
        aggr_child_bytes = 0
        num_childs = len(childs)
        for child_number, ch in enumerate(childs, start=1):
            if not ch._properties.has_key('record_bytes'):
                continue

            ch._is_local_top = True
            aggr_child_bytes += ch._properties['record_bytes']
            delta_bytes = parent_bytes - aggr_child_bytes
            if (delta_bytes > 0 and (aggr_child_bytes / float(parent_bytes)) > 0.8 and num_childs > 5) or child_number == 10:
                delta_record = RecordMetadata('others', parent_name + '.others', 'N/A', parent._level, parent, alias=None,mode='N/A',
                        is_leaf=True, is_local_top=True, is_dummy=False, date=parent._date, start_of_week=parent._start_of_week, table=parent._table)
                delta_record.add_property('record_bytes', delta_bytes)
                delta_records.append(delta_record)
                break

    return delta_records

def mark_top_records(schema_parser, table_metadata):

    blacklisted_record_names = ['pv', 'pv.requests', 'pv.requests.servedItems']
    top_records = []
    others_record = None
    records = schema_parser.field_name_dictionary.values()
    records.sort(key=lambda r: len(r._name_full)) # sort by secondary key
    records.sort(key=lambda r: r._properties['record_bytes'] if r._properties.has_key('record_bytes') else -1, reverse=True) # sort by primary key, reverse order

    aggr_child_bytes = 0
    num_top_records = 0
    for record in records:
        if not record._properties.has_key('record_bytes'):
            continue

        if record._name_full in blacklisted_record_names:
            continue

        dup_found = False
        for t in top_records:
            if record._name_full.find(t) != -1: #dup found
                dup_found = True
                break

        if dup_found:
            continue

        record._is_global_top = True
        num_top_records +=1
        top_records.append(record._name_full)
        aggr_child_bytes += record._properties['record_bytes']
        delta_bytes = table_metadata._num_bytes - aggr_child_bytes
        if num_top_records == 10:
            others_record = RecordMetadata('others', 'others', 'N/A', 0, None, alias=None, mode='N/A',
                    is_leaf=True, is_local_top=True, is_global_top=True, is_dummy=True, date=record._date, start_of_week=record._start_of_week, table=record._table)
            others_record.add_property('record_bytes', delta_bytes)
            break

    return others_record


def apply(date, args, table_name, meta):
    meta.set_date(date.strftime('%Y-%m-%d'))
    meta.set_start_of_week(calc_start_of_week(date).strftime('%Y-%m-%d'))
    update_record_total_bytes(meta)
    parsed_table_metadata = create_table_metadata(args, table_name)
    meta.set_table(parsed_table_metadata)


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
    parent_parser_1.add_argument('-j', '--out-folder', dest='out_folder', help='output files folder', default='/Users/eran.f/work/python/BqFieldSizeAnalyzer') # TODO: change default
    parent_parser_1.add_argument('--out-dataset', required=True, dest='out_dataset', help='output dataset')
    parent_parser_1.add_argument('--out-project', required=True, dest='out_project', help='output project')
    parent_parser_1.add_argument('--out-table', dest='out_table', help='output table')



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

def create_bq_table_metadata(project, dataset, table_name):


    for i in range(0, 100):
        try:
            table_metadata_json = BqCliDriver.show_table(project, dataset, table_name, 'json')
            metadata = json.loads(table_metadata_json)
        except Exception as e:
            print 'Unable to decode json. reason: {0}, json: {1}, retrying..'.format(e.message, table_metadata_json)
            time.sleep(.300)
            continue
        break

    return metadata
    #build TableMetadata object

def update_record_total_bytes(record_metadata):


    for i in range(0, 100):
        try:
            dry_run_output_json = BqCliDriver.execute_query(record_metadata._queries['recordSizeQuery'], True)
            dry_run_output = json.loads(dry_run_output_json)
        except Exception as e:
            print 'Unable to decode json. reason: {0}, json: {1}, retrying..'.format(e.message, dry_run_output_json)
            time.sleep(.300)
            continue
        break

    query_stats = dry_run_output['statistics']['query']
    total_bytes = int(query_stats['totalBytesProcessed'])
    total_bytes_accuracy = query_stats['totalBytesProcessedAccuracy']
    logging.info('Record %s size: %d bytes', record_metadata._name_full, total_bytes)
    # print  + ' ...',
    # print '{0} bytes'.format(total_bytes)
    record_metadata.add_property('record_bytes', total_bytes)
    record_metadata.add_property('record_bytes_accuracy', total_bytes_accuracy)

def create_table_metadata(args, table_name):
    table_metadata_dict = create_bq_table_metadata(args.project, args.dataset,table_name)

    return TableMetadata(str(table_metadata_dict['id']),
                         table_metadata_dict['schema']['fields'],
                        datetime.datetime.fromtimestamp(int(table_metadata_dict['creationTime']) / 1000.0, tz.gettz('UTC')),
                        datetime.datetime.fromtimestamp(int(table_metadata_dict['lastModifiedTime'])/1000.0, tz.gettz('UTC')),
                        int(table_metadata_dict['numRows']),
                        int(table_metadata_dict['numBytes']))


def create_requested_records_metadata(args, schema_parser):
    command = args.command

    # filter record by criteria
    if command == 'full-scan':
        records = schema_parser.field_name_dictionary.values()
    elif command == 'record-name-scan':
        records = [schema_parser.field_name_dictionary[args.record_name]]
    elif command == 'record-name-deep-scan':
        records = []
        for key, value in schema_parser.field_name_dictionary.iteritems():
            if key.startswith(args.record_name):
                records.append(value)
    elif command == 'record-level-scan':
        records = schema_parser._field_level_dictionary[args.level]
    elif command == 'record-type-scan':
        records = schema_parser._field_type_dictionary[args.record_type]
    elif command == 'record-mode-scan':
        records = schema_parser._field_mode_dictionary[args.record_mode]

    #records_metadata = map(lambda c: RecordMetadata(c), records_schema)
    return records

def add_dummy_record(record_metadata):

    return RecordMetadata(
        name_short=record_metadata._name_short,
        name_full=record_metadata._name_full,
        field_type='N/A',
        level=None,
        mode='N/A',
        description='N/A',
        parent=record_metadata,
        is_leaf=record_metadata._is_leaf,
        is_dummy=True,
        date=record_metadata._date,
        start_of_week=record_metadata._start_of_week,
        queries=record_metadata._queries,
        properties=record_metadata._properties,
        table=record_metadata._table)


def calc_start_of_week(date):
    return date - datetime.timedelta((date.weekday() + 1) % 7)

if __name__ == '__main__':
    main()