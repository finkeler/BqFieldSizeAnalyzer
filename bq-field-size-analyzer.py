import Schema
import json

from JsonSchemaParser import JsonSchemaParser


def main():
    # json_file = open('pageview-simple-json-schema.json')
    # json_str = json_file.read()
    # json_data = json.loads(json_str)
    # field = Schema.SchemaField.from_api_repr(json_data[4])
    #print field

    schemaParser = JsonSchemaParser('pageview-simple-json-schema.json')
    schema = schemaParser.parse()
    print schema


# take a date
# convert to table name

# Schema parsing
## get table schema via bq as json file
## read json as str (via json_file.read() )
## parse json (via json.loads(str) ) to get a list of unformatted dictionaries

## need to hold a dictionary of <full_column_name>:<SchemaFieldObject>
## SchemaFieldObject should hold its own data (including size, level) and also a ref to his parent. this is usefull when we need to traverse the schema to find what we needed to explode in queries

# Get Table metadata (byte size, num rows)

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



if __name__ == '__main__':
    main()