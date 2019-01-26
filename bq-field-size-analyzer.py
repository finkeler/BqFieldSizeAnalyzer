from BqExecuter import BqExecuter
from BqQueryBuilder import BqQueryBuilder
from ColumnMetadata import ColumnMetadata

from JsonSchemaParser import JsonSchemaParser


def main():


    # parse input
    # take date list
    # for each:
    ## convert to table name
    ## Get Table metadata (byte size, num rows)
    ## get table schema via bq as json file
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



if __name__ == '__main__':
    main()