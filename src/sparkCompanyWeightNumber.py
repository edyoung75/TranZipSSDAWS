'''
Insight Data Engineering Project
Version 0.0.01

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Python script to process data

Columns:

CSV output

'''

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


# FUNCTIONS
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
    #print(svrLoc)
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

# How many lines are being processed
linesProcessed = 0

# EXECUTE PROCESSING
if __name__ == "__main__":
    if len(sys.argv) != 12:
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

    schemaCargoDesc = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('description_sequence_number', StringType(), True),
        StructField('piece_count', IntegerType(), True),
        StructField('description_text', StringType(), True)
    ])

    schemaContainer = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('seal_number_1', StringType(), True),
        StructField('seal_number_2', StringType(), True),
        StructField('equipment_description_code', StringType(), True),
        StructField('container_length', IntegerType(), True),
        StructField('container_height', IntegerType(), True),
        StructField('container_width', IntegerType(), True),
        StructField('container_type', StringType(), True),
        StructField('load_status', StringType(), True),
        StructField('type_of_service', StringType(), True)
    ])

    schemaHazmat = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('hazmat_sequence_number', IntegerType(), True),
        StructField('hazmat_code', StringType(), True),
        StructField('hazmat_class', StringType(), True),
        StructField('hazmat_code_qualifier', StringType(), True),
        StructField('hazmat_contact', StringType(), True),
        StructField('hazmat_page_number', StringType(), True),
        StructField('hazmat_flash_point_temperature', StringType(), True),
        StructField('hazmat_flash_point_temperature_negative_ind', StringType(), True),
        StructField('hazmat_flash_point_temperature_unit', StringType(), True),
        StructField('hazmat_description', StringType(), True)
    ])

    schemaHazmatClass = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('hazmat_sequence_number', IntegerType(), True),
        StructField('hazmat_classification', StringType(), True)
    ])

    schemaMarksNumber = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('marks_and_numbers_1', StringType(), True),
        StructField('marks_and_numbers_2', StringType(), True),
        StructField('marks_and_numbers_3', StringType(), True),
        StructField('marks_and_numbers_4', StringType(), True),
        StructField('marks_and_numbers_5', StringType(), True),
        StructField('marks_and_numbers_6', StringType(), True),
        StructField('marks_and_numbers_7', StringType(), True),
        StructField('marks_and_numbers_8', StringType(), True)
    ])

    schemaNotifyParty = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('notify_party_name', StringType(), True),
        StructField('notify_party_address_1', StringType(), True),
        StructField('notify_party_address_2', StringType(), True),
        StructField('notify_party_address_3', StringType(), True),
        StructField('notify_party_address_4', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state_province', StringType(), True),
        StructField('zip_code', StringType(), True),
        StructField('country_code', StringType(), True),
        StructField('contact_name', StringType(), True),
        StructField('comm_number_qualifier', StringType(), True),
        StructField('comm_number', StringType(), True)
    ])

    schemaShipper = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('notify_party_name', StringType(), True),
        StructField('notify_party_address_1', StringType(), True),
        StructField('notify_party_address_2', StringType(), True),
        StructField('notify_party_address_3', StringType(), True),
        StructField('notify_party_address_4', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state_province', StringType(), True),
        StructField('zip_code', StringType(), True),
        StructField('country_code', StringType(), True),
        StructField('contact_name', StringType(), True),
        StructField('comm_number_qualifier', StringType(), True),
        StructField('comm_number', StringType(), True)
    ])

    schemaTariff = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('container_number', StringType(), True),
        StructField('description_sequence_number', StringType(), True),
        StructField('harmonized_number', StringType(), True),
        StructField('harmonized_value', StringType(), True),
        StructField('harmonized_weight', StringType(), True),
        StructField('harmonized_weight_unit', StringType(), True)
    ])

    # File Locations
    csvLocHeader = sys.argv[1]
    csvLocConsignee = sys.argv[2]
    csvLocCargoDesc = sys.argv[3]
    csvLocContainer = sys.argv[4]
    csvLocHazmat = sys.argv[5]
    csvLocHazmatClass = sys.argv[6]
    csvLocMarksNumbers = sys.argv[7]
    csvLocNotifyParty = sys.argv[8]
    csvLocShipper = sys.argv[9]
    csvLocTariff = sys.argv[10]
    filename = sys.argv[11]

    dbname = 'testamsdb'

    #csvLocHeader = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__header_2020__202005251500.csv'
    #csvLocConsignee = 's3://ssd-package-s3-dev/ams/2020/202005251500/ams__consignee_2020__202005251500.csv'

    # Read the files
    spDFHeader = spark.read.csv(csvLocHeader, schema=schemaHeader)
    lineLength = spDFHeader.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHeader,lineLength))
    #spDFHeader.show(n=100)
    spDFConsignee = spark.read.csv(csvLocConsignee, schema=schemaConsignee)
    lineLength = spDFConsignee.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocConsignee, lineLength))
    spDFCargoDesc = spark.read.csv(csvLocCargoDesc, schema=schemaCargoDesc)
    lineLength = spDFCargoDesc.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocCargoDesc, lineLength))
    spDFContainer = spark.read.csv(csvLocContainer, schema=schemaContainer)
    lineLength = spDFContainer.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocContainer, lineLength))
    spDFHazmat = spark.read.csv(csvLocHazmat, schema=schemaHazmat)
    lineLength = spDFHazmat.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHazmat, lineLength))
    spDFHazmatClass = spark.read.csv(csvLocHazmatClass, schema=schemaHazmatClass)
    lineLength = spDFHazmatClass.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHazmatClass, lineLength))
    spDFMarksNumbers = spark.read.csv(csvLocMarksNumbers, schema=schemaMarksNumber)
    lineLength = spDFMarksNumbers.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocMarksNumbers, lineLength))
    spDFNotifyParty = spark.read.csv(csvLocNotifyParty, schema=schemaNotifyParty)
    lineLength = spDFNotifyParty.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocNotifyParty, lineLength))
    spDFShipper = spark.read.csv(csvLocShipper, schema=schemaShipper)
    lineLength = spDFShipper.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocShipper, lineLength))
    spDFTariff = spark.read.csv(csvLocTariff, schema=schemaTariff)
    lineLength = spDFTariff.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocTariff, lineLength))

    # DATA CLEANUP
    # Header file
    spDFHeader = spDFHeader.filter(pyF.col('identifier').isNotNull())
    lineLength = spDFHeader.count()
    print('header line count {}'.format(lineLength))
    #writetofile(spDFHeader, 'ssd-test-dev', 'spark/header', 'ports', 'csv')
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
    lineLength = spDFHeader.count()
    print('Writing the header files... {}'.format(lineLength))
    spDFHeader.coalesce(1)
    writetofile(spDFHeader, 'ssd-test-dev', 'spark/header', filename, 'parquet')

    # Consignee File
    spDFConsignee = spDFConsignee.filter(pyF.col('identifier').isNotNull())
    spDFConsignee = spDFConsignee.withColumn('country_code', pyF.when(pyF.col('country_code').isNull(), countrycode('consignee_name')).otherwise(spDFConsignee['country_code']))
    #spDFConsignee.show()
    writetofile(spDFConsignee, 'ssd-test-dev', 'spark/consignee', filename, 'parquet')

    # Cargo Description
    spDFCargoDesc = spDFCargoDesc.filter(pyF.col('identifier').isNotNull())
    # tfilter = spDFConsignee.filter(pyF.col('country_code').isNull())
    # tfilter.show()
    # print('trying to clean up the country code...')
    # tfilter = spDFConsignee.filter(pyF.col('country_code').isNull())
    # tfilter.show()
    spDFCargoDesc.coalesce(1)
    writetofile(spDFCargoDesc, 'ssd-test-dev', 'spark/cargodesc', filename, 'parquet')

    # Container information
    spDFContainer = spDFContainer.filter(pyF.col('identifier').isNotNull())
    spDFContainer.coalesce(1)
    writetofile(spDFContainer, 'ssd-test-dev', 'spark/container', filename, 'parquet')

    # Hazmat information
    spDFHazmat = spDFHazmat.filter(pyF.col('identifier').isNotNull())
    spDFHazmat.coalesce(1)
    writetofile(spDFHazmat, 'ssd-test-dev', 'spark/hazmat', filename, 'parquet')

    # Hazmat Class information
    spDFHazmatClass = spDFHazmatClass.filter(pyF.col('identifier').isNotNull())
    spDFHazmatClass.coalesce(1)
    writetofile(spDFHazmatClass, 'ssd-test-dev', 'spark/hazmatclass', filename, 'parquet')

    # Marks Numbers on Container information
    spDFMarksNumbers = spDFMarksNumbers.filter(pyF.col('identifier').isNotNull())
    spDFMarksNumbers.coalesce(1)
    writetofile(spDFMarksNumbers, 'ssd-test-dev', 'spark/marksnumbers', filename, 'parquet')

    # Notify Party information
    spDFNotifyParty = spDFNotifyParty.filter(pyF.col('identifier').isNotNull())
    spDFNotifyParty.coalesce(1)
    writetofile(spDFNotifyParty, 'ssd-test-dev', 'spark/notifyparty', filename, 'parquet')

    # Shipper information
    spDFShipper = spDFShipper.filter(pyF.col('identifier').isNotNull())
    spDFShipper.coalesce(1)
    writetofile(spDFShipper, 'ssd-test-dev', 'spark/shipper', filename, 'parquet')

    # Tariff information
    spDFTariff = spDFTariff.filter(pyF.col('identifier').isNotNull())
    spDFTariff.coalesce(1)
    writetofile(spDFTariff, 'ssd-test-dev', 'spark/tariff', filename, 'parquet')


    #spDFNoConsigneeName = spDFConsignee.filter(pyF.col('consignee_name').isNull() & pyF.col('identifier').isNotNull())
    #spDFNoConsigneeName.show()
    #lineLength = spDFNoConsigneeName.count()
    #print('Dataset {} has {} rows'.format('NoConsigneeNameDF', lineLength))
    #spDFNoCountry = spDFConsignee.select(pyF.col('country_code').isNull())
    # spDFNoCountry = spDFConsignee.filter(pyF.col('country_code').isNull() & pyF.col('consignee_name').isNotNull())
    #spDFNoCountry.show()
    #spListNoCountryCollection = spDFNoCountry.collect()
    #spDFNoCountryCollection = spark.createDataFrame(spListNoCountryCollection, schemaConsignee)
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
    print('Lines Processed: {}'.format(linesProcessed), file=sys.stdout)
    print('End time: %d seconds (epoch)' % (nEndTime), file=sys.stdout)
    print('Time elapsed: %d seconds' % ((nEndTime - nStartTime)), file=sys.stdout)
    spark.stop()