

from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import when, to_date, datediff, year, month, dayofmonth
import pyspark.sql.functions as pyF
import pyspark.sql.functions
import pandas as pd
import boto3
import inspect, os
import time
from datetime import datetime
import re

s3 = boto3.client('s3')
#sc = spark.sparkContext

# Start Time
nStartTime = time.time()   # Time start
print('Start Time:'.format(nStartTime), file=sys.stdout)

def lbstokg(lbs):
    kilos = lbs/2.2046
    return kilos

def datediff2(earlydate, latedate):
    #d1 = datetime.strptime(earlydate, "%Y-%m-%d")
    #d2 = datetime.strptime(latedate, "%Y-%m-%d")
    return abs((latedate - earlydate).days)

def writetofile(dataframe, bucket, location, foldername, filetype):
    svrLoc = 's3://{}/{}/{}'.format(bucket,location,foldername)
    dataframe.cache()
    print(svrLoc)
    if filetype == 'csv':
        dataframe.write.mode('overwrite').csv(svrLoc)
    else:
        dataframe.write.mode('overwrite').parquet(svrLoc)

exemptionCoDict = {'PERLITE CANADA': 'CA',
                   'GRAND RIVER ENTERPRISES SIX NATIONS': 'CA',
                   'PRIME SHIPPING INTERNATIONAL': 'US',
                   'PANTOS LOGISTICS CANADA INC.': 'CA',
                   'GB PLASTIQUE INC': 'CA',
                   'CYTEC CANADA INC.': 'CA',
                   'MOTORAMBAR INC.': 'PR',
                   'TOP-CO INC.': 'CA',
                   'HI TECH SEALS INC': 'CA',
                   'D&H CANADA': 'CA',
                   'D & H CANADA ULC': 'CA',
                   'ROCK SOLID SUPPLY': 'CA',
                   'INVACARE CANADA L.P.': 'CA',
                   'CANADIAN THERMOS PRODUCTS INC.': 'CA'
                   }

def countrycode(companyname):
    result = 'US'
    for name, code in exemptionCoDict.items():
        if companyname == name:
            #print('match')
            result = code
    #print(result)
    return result

if __name__ == "__main__":
    if len(sys.argv) != 7:
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
        StructField('estimated_arrival_date', DateType(), True),
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
        StructField('actual_arrival_date', DateType(), True)
    ])

    # File Locations
    csvLocHeader = sys.argv[1]
    csvLocConsignee = sys.argv[2]
    filename = sys.argv[3]
    password = sys.argv[4]
    port = sys.argv[5]
    host = sys.argv[6]
    dbname = 'testamsdb'

    #csvLocHeader = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__header_2020__202005251500.csv'
    #csvLocConsignee = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__consignee_2020__202005251500.csv'

    # Read the files
    spDFHeader = spark.read.csv(csvLocHeader, schema=schemaHeader)
    lineLength = spDFHeader.count()
    print('File {} has {} rows'.format(csvLocHeader,lineLength))
    #spDFHeader.show(n=100)
    spDFConsignee = spark.read.csv(csvLocConsignee, schema=schemaConsignee)
    lineLength = spDFConsignee.count()
    print('File {} has {} rows'.format(csvLocConsignee, lineLength))

    # DATA CLEANUP
    # Header file
    spDFHeader = spDFHeader.withColumn('weight', pyF.when(spDFHeader['weight_unit'] == 'Pounds', lbstokg(spDFHeader['weight'])).otherwise(spDFHeader['weight']))
    spDFHeader = spDFHeader.withColumn('weight_unit', pyF.when(spDFHeader['weight_unit'] == 'Pounds', 'Kilograms').otherwise(spDFHeader['weight_unit']))
    spDFHeader = spDFHeader.withColumn('estimated_arrival_date', pyF.to_date('estimated_arrival_date', 'yyyy-MM-dd').cast(DateType()))
    spDFHeader = spDFHeader.withColumn('actual_arrival_date', pyF.to_date('actual_arrival_date', 'yyyy-MM-dd').cast(DateType()))
    spListHeaderPorts = spDFHeader.select('foreign_port_of_lading').distinct().collect()
    spDFHeaderPorts = spark.createDataFrame(spListHeaderPorts, StructType([StructField('ports', StringType(), True)]))
    # writetofile(spDFHeaderPorts, 'ssd-test-dev', 'spark/consignee', 'ports', 'csv')
    # print('wrote out port csv')
    # Append the date difference between the Arrival date and the Estimate date
    spDFHeader = spDFHeader.withColumn('date_diff', datediff(spDFHeader['actual_arrival_date'], spDFHeader['estimated_arrival_date']))
    spDFHeader = spDFHeader.filter(pyF.col('identifier').isNotNull())
    lineLength = spDFHeader.count()
    print('Writing the header files... {}'.format(lineLength))
    spDFHeader.coalesce(1)
    writetofile(spDFHeader, 'ssd-test-dev', 'spark/header', filename, 'parquet')

    # Consignee File
    # tfilter = spDFConsignee.filter(pyF.col('country_code').isNull())
    # tfilter.show()
    # print('trying to clean up the country code...')
    spDFConsignee = spDFConsignee.withColumn('country_code', pyF.when(pyF.col('country_code').isNull(), countrycode('consignee_name')).otherwise(spDFConsignee['country_code']))
    spDFConsignee = spDFConsignee.filter(pyF.col('identifier').isNotNull())
    spDFConsignee.show()
    # tfilter = spDFConsignee.filter(pyF.col('country_code').isNull())
    # tfilter.show()

    spDFNoConsigneeName = spDFConsignee.filter(pyF.col('consignee_name').isNull() & pyF.col('identifier').isNotNull())
    spDFNoConsigneeName.show()
    lineLength = spDFNoConsigneeName.count()
    print('Dataset {} has {} rows'.format('NoConsigneeNameDF', lineLength))
    spDFNoCountry = spDFConsignee.select(pyF.col('country_code').isNull())
    # spDFNoCountry = spDFConsignee.filter(pyF.col('country_code').isNull() & pyF.col('consignee_name').isNotNull())
    spDFNoCountry.show()
    spListNoCountryCollection = spDFNoCountry.collect()
    #spDFNoCountryCollection = spark.createDataFrame(spListNoCountryCollection, schemaConsignee)

    #writetofile(spDFNoCountryCollection,'ssd-test-dev','spark/consignee',filename,'csv')
    # Show Content Output
    #spDFHeader.show(n=100)

    #lines = spark.read.text(csvLocHeader).rdd.map(lambda r: r[0])
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)
    # output = counts.collect()

    # Convert list to RDD
    #rdd = spark.sparkContext.parallelize(output)

    # Print data frame:
    # print(spDFHeader.schema, file=sys.stdout)
    # spDFHeader.show()
    # print(spDFConsignee.schema, file=sys.stdout)
    # spDFConsignee.show()

    # # Join Header with Consignee
    # spDFHeaderConsignee = spDFHeader.join(spDFConsignee, on=['identifier'], how='inner')
    # lineLength = spDFHeaderConsignee.count()
    # print('Dataset {} has {} rows'.format('header_consignee', lineLength))
    # # sqlContext.sql("""
    # #     SELECT MONTH(timestamp) AS month, SUM(value) AS values_sum
    # #     FROM df
    # #     GROUP BY MONTH(timestamp)""")
    #
    # counts = spDFHeaderConsignee.rdd.map(lambda r: r['consignee_name'])\
    #     .map(lambda x: (x, 1))\
    #     .reduceByKey(add)
    #
    # output = counts.collect()
    # # Convert list to RDD
    # rddHeaderConsigneeCounts = spark.sparkContext.parallelize(output)
    #
    # schemaHeaderConsigneeCounts = StructType([
    #     StructField('company', StringType(), True),
    #     StructField('package_counts', IntegerType(), True)
    # ])
    #
    # # Create data frame
    # df = spark.createDataFrame(rddHeaderConsigneeCounts, schemaHeaderConsigneeCounts)
    # #df.sort('package_counts')
    # lineLength = df.count()
    # print('Dataframe {} has {} rows'.format('header_consignee counts', lineLength))
    # df.show()
    #
    # # counts = lines.flatMap(lambda x: x.split(' ')) \
    # #               .map(lambda x: (x, 1)) \
    # #               .reduceByKey(add)
    # # output = counts.collect()
    #
    # # for (word, count) in output:
    # #     print("%s: %i" % (word, count), file=sys.stdout)
    #
    # # Output the files to Parquet
    # # bucket = 'ssd-test-dev'
    # n01Time = time.time()
    # filename = 'testoutput%d.parquet' % (n01Time)
    # #filename = 'testoutput%d.csv' % (n01Time)
    # # k = "spark/output/" + filename
    # # svrLoc = 's3://ssd-test-dev/spark/output/'
    # # k2 = svrLoc + filename
    # # df.write.mode('overwrite').parquet(k2)
    # #df.write.mode('overwrite').csv(k2)
    # writetofile(df, 'ssd-test-dev', 'spark/output', filename, 'parquet')

    # End Time
    nEndTime = time.time()  # Time start
    print("End time: %d seconds (epoch)" % (nEndTime), file=sys.stdout)
    print("Time elapsed: %d seconds" % ((nEndTime - nStartTime)), file=sys.stdout)
    spark.stop()