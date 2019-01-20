import Schema
import json

def main():
    json_file = open('pageview-simple-json-schema.json')
    json_str = json_file.read()
    #json_str = '[{"mode":"NULLABLE","name":"top1","type":"TIMESTAMP"},{"fields":[{"mode":"NULLABLE","name":"son1","type":"BOOLEAN"},{"fields":[{"mode":"NULLABLE","name":"t1","type":"BOOLEAN"},{"mode":"NULLABLE","name":"t2","type":"BOOLEAN"}],"mode":"NULLABLE","name":"request","type":"RECORD"},{"mode":"NULLABLE","name":"son3","type":"BOOLEAN"}],"mode":"NULLABLE","name":"pv","type":"RECORD"}]'

    json_data = json.loads(json_str)
    #json_data_0 = json.loads(json_str)[4]
    #field = schema.SchemaField.from_api_repr(json_data_0)

    field = Schema.SchemaField.from_api_repr(json_data[4])
    #print field

    print 1


# take a date
# convert to table name
# get table schema via bq as json file
# read json as str (via json_file.read() )
# parse json (via json.loads(str) ) to get a list of unformatted dictionaries

# need to hold a dictionary of <full_column_name>:<SchemaFieldObject>
# SchemaFieldObject should hold its own data (including size, level) and also a ref to his parent. this is usefull when we need to traverse the schema to find what we needed to explode in queries

# once there is a dictionary we could:
# a. get a single column name (fully qualified) as input
# b. traverse all columns and create queries
# c. traverse by level
# d. traverse by type / and specifically - lists/strings


if __name__ == '__main__':
    main()