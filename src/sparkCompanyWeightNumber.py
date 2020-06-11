

from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import pandas as pd
import boto3
import inspect, os
import time

s3 = boto3.client('s3')
#sc = spark.sparkContext

# Start Time
nStartTime = time.time()   # Time start
print('Start Time:'.format(nStartTime), file=sys.stdout)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sparkCompanyWeightNumber <file> <file>", file=sys.stderr)
        sys.exit(-1)

    # Create the Spark Session
    spark = SparkSession\
        .builder\
        .appName("SparkCompanyWeightNumber")\
        .getOrCreate()

    # SCHEMAS:
    # Schema Type Reference: https://spark.apache.org/docs/latest/sql-reference.html
    schemaConsignee = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('consignee_name', StringType(), True),
        StructField('consignee_address_1', StringType(), True),
        StructField('consignee_address_2', StringType(), True),
        StructField('consignee_address_3', StringType(), True),
        StructField('consignee_address_4', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state_province', StringType(), True),
        StructField('zip_code', StringType(), True),
        StructField('country_code', StringType(), True),
        StructField('contact_name', StringType(), True),
        StructField('comm_number_qualifier', StringType(), True),
        StructField('comm_number', StringType(), True)
    ])

    schemaHeader = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('carrier_code', StringType(), True),
        StructField('vessel_country_code', StringType(), True),
        StructField('vessel_name', StringType(), True),
        StructField('port_of_unlading', StringType(), True),
        StructField('estimated_arrival_date', StringType(), True),
        StructField('foreign_port_of_lading_qualifier', StringType(), True),
        StructField('foreign_port_of_lading', StringType(), True),
        StructField('manifest_quantity', IntegerType(), True),
        StructField('manifest_unit', StringType(), True),
        StructField('weight', IntegerType(), True),
        StructField('weight_unit', StringType(), True),
        StructField('measurement', IntegerType(), True),
        StructField('measurement_unit', StringType(), True),
        StructField('record_status_indicator', StringType(), True),
        StructField('place_of_receipt', StringType(), True),
        StructField('port_of_destination', StringType(), True),
        StructField('foreign_port_of_destination_qualifier', StringType(), True),
        StructField('foreign_port_of_destination', StringType(), True),
        StructField('conveyance_id_qualifier', StringType(), True),
        StructField('conveyance_id', IntegerType(), True),
        StructField('in_bond_entry_type', StringType(), True),
        StructField('mode_of_transportation', StringType(), True),
        StructField('secondary_notify_party_1', StringType(), True),
        StructField('secondary_notify_party_2', StringType(), True),
        StructField('secondary_notify_party_3', StringType(), True),
        StructField('secondary_notify_party_4', StringType(), True),
        StructField('secondary_notify_party_5', StringType(), True),
        StructField('secondary_notify_party_6', StringType(), True),
        StructField('secondary_notify_party_7', StringType(), True),
        StructField('secondary_notify_party_8', StringType(), True),
        StructField('secondary_notify_party_9', StringType(), True),
        StructField('secondary_notify_party_10', StringType(), True),
        StructField('actual_arrival_date', StringType(), True)
    ])

    # File Locations
    csvLocHeader = sys.argv[1]
    csvLocConsignee = sys.argv[2]
    #csvLocHeader = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__header_2020__202005251500.csv'
    #csvLocConsignee = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__consignee_2020__202005251500.csv'

    # Read the files
    spDFHeader = spark.read.csv(csvLocHeader, schema=schemaHeader)
    spDFConsignee = spark.read.csv(csvLocConsignee, schema=schemaConsignee)

    #lines = spark.read.text(csvLocHeader).rdd.map(lambda r: r[0])
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)
    # output = counts.collect()

    # Convert list to RDD
    #rdd = spark.sparkContext.parallelize(output)

    # Create data frame
    #df = spark.createDataFrame(rdd, schema)
    print(spDFHeader.schema, file=sys.stdout)
    spDFHeader.show()
    print(spDFConsignee.schema, file=sys.stdout)
    spDFConsignee.show()

    # Join Header with Consignee
    spDFHeaderConsignee = spDFHeader.join(spDFConsignee, spDFHeader.identifier == spDFConsignee.identifier)
    spDFHeaderConsignee.show()

    counts = spDFHeaderConsignee.rdd.map(lambda r: r['consignee_name'])\
        .map(lambda x: (x, 1))\
        .reduceByKey(add)

    output = counts.collect()
    # Convert list to RDD
    rddHeaderConsigneeCounts = spark.sparkContext.parallelize(output)

    schemaHeaderConsigneeCounts = StructType([
        StructField('company', StringType(), True),
        StructField('package_counts', IntegerType(), True)
    ])
    # Create data frame
    df = spark.createDataFrame(rddHeaderConsigneeCounts, schemaHeaderConsigneeCounts)
    df.show()

    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)
    # output = counts.collect()

    # for (word, count) in output:
    #     print("%s: %i" % (word, count), file=sys.stdout)

    # Output the files to Parquet
    # bucket = 'ssd-test-dev'
    # filename = 'testoutput.parquet'
    # k = "spark/output/" + filename
    # svrLoc = 's3://ssd-test-dev/spark/output/'
    # k2 = svrLoc + filename
    # df.write.mode('overwrite').parquet(k2)

    # End Time
    nEndTime = time.time()  # Time start
    print("End time: %d seconds (epoch)" % (nEndTime), file=sys.stdout)
    print("Time elapsed: %d seconds" % ((nEndTime - nStartTime)), file=sys.stdout)
    spark.stop()