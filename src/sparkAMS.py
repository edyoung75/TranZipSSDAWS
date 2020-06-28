'''
Insight Data Engineering Project
Version 0.0.12

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
PySpark script to process data


'''

from __future__ import print_function
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
import pyspark.sql.functions as sqlF
import boto3
import time

# Create S3 Client
s3 = boto3.client('s3')

# Start Time
nStartTime = time.time()  # Time start
print('Start Time:'.format(nStartTime), file=sys.stdout)


# FUNCTIONS:
# Change lbs to kg
def lbstokg(lbs):
    kilos = lbs / 2.2046
    return kilos

# Write out the dataframe to disk
def writetofile(dataframe, bucket, location, foldername, filetype):
    svrLoc = 's3://{}/{}/{}'.format(bucket, location, foldername)
    dataframe.cache()
    # print(svrLoc)
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

exPortDist = {
    'SHANGHAI': 'SHANGHAI,CHINA',
    'CNTAO': 'QINGDAO'
}

def countrycode(companyname):
    result = 'US'
    for name, code in exemptionCoDict.items():
        if companyname == name:
            # print('match')
            result = code
    # print(result)
    return result

def matchvalue(value, checkdict, defaultvalue):
    result = defaultvalue
    for name, returnvalue in checkdict.items():
        if value == name:
            # print('match')
            result = returnvalue
    # print(result)
    return result

# How many lines are being processed
linesProcessed = 0

# EXECUTE PROCESSING:
if __name__ == "__main__":
    if len(sys.argv) != 35:
        print("Usage: sparkAMS <file x33> <output_file_name>", file=sys.stderr)
        sys.exit(-1)

    # Create the Spark Session
    spark = SparkSession \
        .builder \
        .appName("SparkCompanyWeightNumber") \
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
        StructField('harmonized_weight', IntegerType(), True),
        StructField('harmonized_weight_unit', StringType(), True)
    ])

    schemaBillGen = StructType([
        StructField('identifier', IntegerType(), True),
        StructField('master_bol_number', StringType(), True),
        StructField('house_bol_number', StringType(), True),
        StructField('sub_house_bol_number', StringType(), True),
        StructField('voyage_number', StringType(), True),
        StructField('bill_type_code', StringType(), True),
        StructField('manifest_number', StringType(), True),
        StructField('trade_update_date', StringType(), True),
        StructField('run_date', StringType(), True)
    ])

    # VARIABLES:
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
    csvLocBillGen = sys.argv[11]
    csvLocHeader1 = sys.argv[12]
    csvLocConsignee1 = sys.argv[13]
    csvLocCargoDesc1 = sys.argv[14]
    csvLocContainer1 = sys.argv[15]
    csvLocHazmat1 = sys.argv[16]
    csvLocHazmatClass1 = sys.argv[17]
    csvLocMarksNumbers1 = sys.argv[18]
    csvLocNotifyParty1 = sys.argv[19]
    csvLocShipper1 = sys.argv[20]
    csvLocTariff1 = sys.argv[21]
    csvLocBillGen1 = sys.argv[22]
    csvLocHeader2 = sys.argv[23]
    csvLocConsignee2 = sys.argv[24]
    csvLocCargoDesc2 = sys.argv[25]
    csvLocContainer2 = sys.argv[26]
    csvLocHazmat2 = sys.argv[27]
    csvLocHazmatClass2 = sys.argv[28]
    csvLocMarksNumbers2 = sys.argv[29]
    csvLocNotifyParty2 = sys.argv[30]
    csvLocShipper2 = sys.argv[31]
    csvLocTariff2 = sys.argv[32]
    csvLocBillGen2 = sys.argv[33]
    filename = sys.argv[34]

    # Database
    dbname = 'testamsdb'

    # FUNCTIONS:
    def readAndUnion(filelist, tschema):
        df = spark.createDataFrame([], schema=tschema)
        for file in filelist:
            df1 = spark.read.csv(file, schema=tschema)
            df = df.union(df1)
        return df

    # READ FILES AND CREATE DATAFRAME:
    # Header File
    tlist = [csvLocHeader, csvLocHeader1, csvLocHeader2]
    spDFHeader = readAndUnion(tlist, schemaHeader)
    lineLength = spDFHeader.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHeader, lineLength))

    # Consignee File
    tlist = [csvLocConsignee, csvLocConsignee1, csvLocConsignee2]
    spDFConsignee = readAndUnion(tlist, schemaConsignee)
    lineLength = spDFConsignee.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocConsignee, lineLength))

    # Cargo Description File
    tlist = [csvLocCargoDesc, csvLocCargoDesc1, csvLocCargoDesc2]
    spDFCargoDesc = readAndUnion(tlist, schemaCargoDesc)
    lineLength = spDFCargoDesc.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocCargoDesc, lineLength))

    # Container File
    tlist = [csvLocContainer, csvLocContainer1, csvLocContainer2]
    spDFContainer = readAndUnion(tlist, schemaContainer)
    lineLength = spDFContainer.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocContainer, lineLength))

    # Hazmat File
    tlist = [csvLocHazmat, csvLocHazmat1, csvLocHazmat2]
    spDFHazmat = readAndUnion(tlist, schemaHazmat)
    lineLength = spDFHazmat.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHazmat, lineLength))

    # Hazmat Class File
    tlist = [csvLocHazmatClass, csvLocHazmatClass1, csvLocHazmatClass2]
    spDFHazmatClass = readAndUnion(tlist, schemaHazmatClass)
    lineLength = spDFHazmatClass.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocHazmatClass, lineLength))

    # Marks and Numbers on Container File
    tlist = [csvLocMarksNumbers, csvLocMarksNumbers1, csvLocMarksNumbers2]
    spDFMarksNumbers = readAndUnion(tlist, schemaMarksNumber)
    lineLength = spDFMarksNumbers.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocMarksNumbers, lineLength))

    # Notify Party File
    tlist = [csvLocNotifyParty, csvLocNotifyParty1, csvLocNotifyParty2]
    spDFNotifyParty = readAndUnion(tlist, schemaNotifyParty)
    lineLength = spDFNotifyParty.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocNotifyParty, lineLength))

    # Shipper File
    tlist = [csvLocShipper, csvLocShipper1, csvLocShipper2]
    spDFShipper = readAndUnion(tlist, schemaShipper)
    lineLength = spDFShipper.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocShipper, lineLength))

    # Tariff File
    tlist = [csvLocTariff, csvLocTariff1, csvLocTariff2]
    spDFTariff = readAndUnion(tlist, schemaTariff)
    lineLength = spDFTariff.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocTariff, lineLength))

    # Bill Generated File
    tlist = [csvLocBillGen, csvLocBillGen1, csvLocBillGen2]
    spDFBillGen = readAndUnion(tlist, schemaBillGen)
    lineLength = spDFBillGen.count()
    linesProcessed += lineLength
    print('File {} has {} rows'.format(csvLocBillGen, lineLength))



    # DATA CLEANUP:
    # Header Clean Up
    spDFHeader = spDFHeader.withColumn('weight',
                                       sqlF.when(spDFHeader['weight_unit'] == 'Pounds',
                                                lbstokg(spDFHeader['weight'])).otherwise(spDFHeader['weight'])
                                       )
    spDFHeader = spDFHeader.withColumn('weight_unit',
                                       sqlF.when(spDFHeader['weight_unit'] == 'Pounds',
                                                'Kilograms').otherwise(spDFHeader['weight_unit'])
                                       )
    spDFHeader = spDFHeader.withColumn('estimated_arrival_date',
                                       sqlF.to_date('estimated_arrival_date', 'yyyy-MM-dd').cast(DateType())
                                       )
    spDFHeader = spDFHeader.withColumn('actual_arrival_date',
                                       sqlF.to_date('actual_arrival_date', 'yyyy-MM-dd').cast(DateType())
                                       )

    # Append the date difference between the Arrival date and the Estimate date
    spDFHeader = spDFHeader.withColumn('place_of_receipt',
                                       sqlF.when((spDFHeader['place_of_receipt'] == 'CNTAO') | (spDFHeader['place_of_receipt'] == 'SHANGHAI'),
                                                 matchvalue('place_of_receipt',
                                                            exPortDist,
                                                            'place_of_receipt')).otherwise(spDFHeader['place_of_receipt'])
                                       )
    spDFHeader = spDFHeader.withColumn('date_diff',
                                       sqlF.datediff(spDFHeader['actual_arrival_date'],
                                                spDFHeader['estimated_arrival_date'])
                                       )

    # Consignee Clean Up
    spDFConsignee = spDFConsignee.withColumn('country_code',
                                             sqlF.when(sqlF.col('country_code').isNull(),
                                                       countrycode('consignee_name')).otherwise(spDFConsignee['country_code'])
                                             )

    # Bill Gen Clean Up
    spDFBillGen = spDFBillGen.withColumn('trade_update_date',
                                       sqlF.to_date('trade_update_date', 'yyyy-MM-dd').cast(DateType())
                                         )
    spDFBillGen = spDFBillGen.withColumn('run_date',
                                         sqlF.to_date('run_date', 'yyyy-MM-dd').cast(DateType())
                                         )

    # Tariff Clean Up
    spDFTariff = spDFTariff.withColumn('harmonized_weight',
                                       sqlF.when(spDFTariff['harmonized_weight_unit'] == 'Pounds',
                                                 lbstokg(spDFTariff['harmonized_weight'])).otherwise(spDFTariff['harmonized_weight'])
                                       )
    spDFTariff = spDFTariff.withColumn('harmonized_weight_unit',
                                       sqlF.when(spDFTariff['harmonized_weight_unit'] == 'Pounds',
                                                 'Kilograms').otherwise(spDFTariff['harmonized_weight_unit'])
                                       )
    #spDFTariff.show(200)

    # SUMMARIZE DATA:
    # Summarize Month/Weight
    spDFSumMonthWeight = spDFHeader.groupby(
        sqlF.date_format('actual_arrival_date', 'yyyy-MM').alias('arrival_date_year_month')
    ).agg(
        sqlF.sum('weight').alias('sum_weight'),
        sqlF.mean('weight').alias('average_weight'),
        sqlF.max('weight').alias('max_weight'),
        sqlF.min('weight').alias('min_weight')
    )
    #spDFSumMonthWeight.show()

    # Summarize Port/Weight
    spDFSumPortWeight = spDFHeader.groupby(
        'foreign_port_of_lading',
        sqlF.date_format('actual_arrival_date', 'yyyy-MM').alias('arrival_date_year_month')
    ).agg(
        sqlF.sum('weight').alias('sum_weight'),
        sqlF.mean('weight').alias('average_weight'),
        sqlF.max('weight').alias('max_weight'),
        sqlF.min('weight').alias('min_weight')
    )
    #spDFSumPortWeight.show()

    # Join Header with Consignee, Cargo Description, Shipper
    spDFHeaderConsignee = spDFHeader.join(spDFConsignee,
                                          on=['identifier'],
                                          how='outer')
    spDFHeaderConsigneeCargoDesc = spDFHeaderConsignee.join(spDFCargoDesc,
                                                            on=['identifier'],
                                                            how='outer')
    spDFHeaderConsigneeCargoDescShipper = spDFHeaderConsigneeCargoDesc.join(spDFShipper,
                                                                            on=['identifier'],
                                                                            how='outer')
    #spDFHeaderConsigneeCargoDescShipper.show()

    # Summarize Consignee/Weight
    spDFSumConsigneeWeight = spDFHeaderConsigneeCargoDescShipper.groupby(
        'consignee_name',
        'foreign_port_of_lading',
        'notify_party_name',
        'description_text',
        sqlF.date_format('actual_arrival_date', 'yyyy-MM').alias('arrival_date_year_month')
    ).agg(
        sqlF.sum('weight').alias('sum_weight'),
        sqlF.mean('weight').alias('average_weight'),
        sqlF.max('weight').alias('max_weight'),
        sqlF.min('weight').alias('min_weight')
    )
    spDFSumConsigneeWeight.show()

    # WRITE OUT FILES
    # Header Write Out

    writeDict = {
        'header': [spDFHeader, 'parquet'],
        'consignee': [spDFConsignee, 'parquet'],
        'cargodesc': [spDFCargoDesc, 'parquet'],
        'container': [spDFContainer, 'parquet'],
        'hazmat': [spDFHazmat, 'parquet'],
        'hazmatclass': [spDFHazmatClass, 'parquet'],
        'marksnumbers': [spDFMarksNumbers, 'parquet'],
        'notifyparty': [spDFNotifyParty, 'parquet'],
        'shipper': [spDFShipper, 'parquet'],
        'tariff': [spDFTariff, 'parquet'],
        'billgen': [spDFBillGen, 'parquet']
    }
    print('new version')
    for foldername, writeoutlist in writeDict.items():
        writeoutlist[0] = writeoutlist[0].filter(sqlF.col('identifier').isNotNull())
        # spDFHeader.coalesce(1)
        writetofile(writeoutlist[0], 'ssd-test-dev', 'spark/'+foldername, filename, writeoutlist[1])

    # Summary Weight/Month Write Out
    writetofile(spDFSumMonthWeight, 'ssd-test-dev', 'spark/summonthweight', filename, 'parquet')

    # Summary Port/Weight/Year Write Out
    writetofile(spDFSumPortWeight, 'ssd-test-dev', 'spark/sumportweight', filename, 'parquet')

    # Summary Consignee/Weight/Year Write Out
    writetofile(spDFSumConsigneeWeight, 'ssd-test-dev', 'spark/sumconsigneeweight', filename, 'parquet')

    # End Time
    nEndTime = time.time()  # Time start
    print('Lines Processed: {}'.format(linesProcessed), file=sys.stdout)
    print('End time: %d seconds (epoch)' % (nEndTime), file=sys.stdout)
    print('Time elapsed: %d seconds' % ((nEndTime - nStartTime)), file=sys.stdout)

    # STOP SPARK
    spark.stop()
