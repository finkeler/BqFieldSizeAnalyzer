import subprocess
import time


def execute_query(query, is_dry_run_query, use_legacy_sql=False, output_format='json'):

    command = ['bq', 'query']
    command.extend(['--format=' + output_format])
    command.extend(['--use_legacy_sql='+ str(use_legacy_sql)])
    if is_dry_run_query:
        command.extend(['--dry_run'])
    command.extend([query])

    return __execute_command(command)


def show_table(project, dataset, table_name, output_format='json'):
    # bq show --format=json taboola-data:pageviews.pageviews_20190122
    command = ['bq', 'show']
    command.extend(['--format='+output_format])
    command.extend([project + ':' + dataset + '.' + table_name])

    return __execute_command(command)


def load_table(project, dataset, table_name, path_to_source, is_day_partitioning, time_partitioning_field, write_preference='replace', source_format='NEWLINE_DELIMITED_JSON', output_format='json'):

    #bq --location=[LOCATION] load --source_format=[FORMAT] [DATASET].[TABLE] [PATH_TO_SOURCE] [SCHEMA]
    command = ['bq', 'load', '--autodetect']
        #, '--schema_update_option=ALLOW_FIELD_ADDITION']
    command.extend(['--project_id=' + project])
    command.extend(['--format=' + output_format])
    command.extend(['--' + write_preference])
    command.extend(['--source_format=' + source_format])

    if is_day_partitioning:
        command.extend(['--time_partitioning_type=DAY'])
        command.extend(['--time_partitioning_field=' + time_partitioning_field])

    command.extend([dataset + '.' + table_name, path_to_source])

    ## check what is the output if using format=json

    #print 'load command: ', command
    return __execute_command(command)


def __execute_command(command):
    for i in range(0, 5):
        try:
            return subprocess.Popen(command, shell=False, stdout=subprocess.PIPE).communicate()[0]
        except Exception as e:
            print 'Failed to execute command. reason: {0}. retrying..'.format(e.message)
            time.sleep(.300)
            continue