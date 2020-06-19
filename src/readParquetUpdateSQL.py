'''
Insight Data Engineering Project
Version 0.0.01

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Reads the parquet files and updates the data warehouse

Columns:

CSV output

'''

import pandas as pd
import pyarrow.parquet as pq
import awswrangler as wr
import psycopg2
import sqlalchemy
# import s3fs
import special.getDataContent as gdc
import sys
import time

# Start Time
nStartTime = time.time()   # Time start
print('Start Time:'.format(nStartTime), file=sys.stdout)

# Get Encrypted DB information
username, password, port, host = gdc.psqlDataContent()

# PSQL Database to use
dbase = 'testamsdb'

# File list to parse and process into the PSQL DB
#fileLoc = 's3://ssd-test-dev/spark/header/2018'
fileHeaderList = [
    's3://ssd-test-dev/spark/header/2018',
    's3://ssd-test-dev/spark/header/2019',
    's3://ssd-test-dev/spark/header/2020'
]

fileConsigneeList = [
    's3://ssd-test-dev/spark/consignee/2018',
    's3://ssd-test-dev/spark/consignee/2019',
    's3://ssd-test-dev/spark/consignee/2020'
]

fileCargoDescList = [
    's3://ssd-test-dev/spark/cargodesc/2018',
    's3://ssd-test-dev/spark/cargodesc/2019',
    's3://ssd-test-dev/spark/cargodesc/2020'
]

fileContainerList = [
    's3://ssd-test-dev/spark/container/2018',
    's3://ssd-test-dev/spark/container/2019',
    's3://ssd-test-dev/spark/container/2020'
]

fileHazmatList = [
    's3://ssd-test-dev/spark/hazmat/2018',
    's3://ssd-test-dev/spark/hazmat/2019',
    's3://ssd-test-dev/spark/hazmat/2020'
]

fileHazmatClassList = [
    's3://ssd-test-dev/spark/hazmatclass/2018',
    's3://ssd-test-dev/spark/hazmatclass/2019',
    's3://ssd-test-dev/spark/hazmatclass/2020'
]

fileMarksNumbersList = [
    's3://ssd-test-dev/spark/marksnumbers/2018',
    's3://ssd-test-dev/spark/marksnumbers/2019',
    's3://ssd-test-dev/spark/marksnumbers/2020'
]

fileNotifyPartyList = [
    's3://ssd-test-dev/spark/notifyparty/2018',
    's3://ssd-test-dev/spark/notifyparty/2019',
    's3://ssd-test-dev/spark/notifyparty/2020'
]

fileShipperList = [
    's3://ssd-test-dev/spark/shipper/2018',
    's3://ssd-test-dev/spark/shipper/2019',
    's3://ssd-test-dev/spark/shipper/2020'
]

fileTariffList = [
    's3://ssd-test-dev/spark/tariff/2018',
    's3://ssd-test-dev/spark/tariff/2019',
    's3://ssd-test-dev/spark/tariff/2020'
]

def getParquetConcatenate(filelist):
    frames = []
    for file in filelist:
        df1 = wr.s3.read_parquet(file, dataset=True)
        frames.append(df1)
    result = pd.concat(frames)
    return result

def tryConnectSQL(pddf, tablename, connection, ifexists):
    try:
        pddf.to_sql(tablename, connection, if_exists=ifexists)
    except ValueError as ve:
        print(ve)
    except Exception as ex:
        print(ex)
    else:
        print("PostgreSQL Table %s has been created successfully." % tablename)
    finally:
        connection.close()

# Create a pandas dataframe from files
dfHeader = getParquetConcatenate(fileHeaderList)
psqlTableNameHeader = 'header'
dfConsignee = getParquetConcatenate(fileConsigneeList)
psqlTableNameConsignee = 'consignee'
dfCargoDesc = getParquetConcatenate(fileCargoDescList)
psqlTableNameCargoDesc = 'cargodesc'
dfContainer = getParquetConcatenate(fileContainerList)
psqlTableNameContainer = 'container'
dfHazmat = getParquetConcatenate(fileHazmatList)
psqlTableNameHazmat = 'hazmat'
dfHazmatClass = getParquetConcatenate(fileHazmatClassList)
psqlTableNameHazmatClass = 'hazmatclass'
dfMarkNumbers = getParquetConcatenate(fileMarksNumbersList)
psqlTableNameMarksNumbers = 'marksnumbers'
dfNotifyParty = getParquetConcatenate(fileNotifyPartyList)
psqlTableNameNotifyParty = 'notifyparty'
dfShipper = getParquetConcatenate(fileShipperList)
psqlTableNameShipper = 'shipper'
dfTariff = getParquetConcatenate(fileTariffList)
psqlTableNameTariff = 'tariff'

#df = wr.s3.read_parquet(fileLoc, dataset=True)

# s3 = s3fs.S3FileSystem()
#
# pandas_dataframe = pq.ParquetDataset(fileLoc, filesystem=s3)
# table = pandas_dataframe.read()
# df = table.to_pandas()

print(dfHeader.count())
print(dfHeader.head())
print(dfConsignee.count())
print(dfConsignee.head())

uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(username,password,host,port,dbase)
alchemyEngine = sqlalchemy.create_engine(uri, pool_recycle=60)
psqlConnection = alchemyEngine.connect()

# delTableSQL = 'DELETE * FROM '

#tryConnectSQL(dfHeader, psqlTableHeader,psqlConnection,'replace')

try:
    dfHeader.to_sql(psqlTableNameHeader, psqlConnection, if_exists='replace')
    dfConsignee.to_sql(psqlTableNameConsignee,psqlConnection, if_exists='replace')
    dfCargoDesc.to_sql(psqlTableNameCargoDesc,psqlConnection, if_exists='replace')
    dfContainer.to_sql(psqlTableNameContainer, psqlConnection, if_exists='replace')
    dfHazmat.to_sql(psqlTableNameHazmat, psqlConnection, if_exists='replace')
    dfHazmatClass.to_sql(psqlTableNameHazmatClass, psqlConnection, if_exists='replace')
    dfMarkNumbers.to_sql(psqlTableNameMarksNumbers, psqlConnection, if_exists='replace')
    dfNotifyParty.to_sql(psqlTableNameNotifyParty, psqlConnection, if_exists='replace')
    dfShipper.to_sql(psqlTableNameShipper, psqlConnection, if_exists='replace')
    dfTariff.to_sql(psqlTableNameTariff, psqlConnection, if_exists='replace')
except ValueError as ve:
    print(ve)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameHeader)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameConsignee)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameCargoDesc)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameContainer)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameHazmat)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameHazmatClass)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameMarksNumbers)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameNotifyParty)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameShipper)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNameTariff)
finally:
    psqlConnection.close()
#
# import psycopg2
# import pandas as pd
# def connect(params_dic):
#     """ Connect to the PostgreSQL database server """
#     conn = None
#     try:
#         # connect to the PostgreSQL server
#         print('Connecting to the PostgreSQL database...')
#         conn = psycopg2.connect(**params_dic)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#         sys.exit(1)
#     return conn
# def single_insert(conn, insert_req):
#     """ Execute a single INSERT request """
#     cursor = conn.cursor()
#     try:
#         cursor.execute(insert_req)
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     cursor.close()
# # Connecting to the database
# conn = connect(param_dic)
# # Inserting each row
# for i in dataframe.index:
#     query = """
#     INSERT into emissions(column1, column2, column3) values('%s',%s,%s);
#     """ % (dataframe['column1'], dataframe['column2'], dataframe['column3'])
#     single_insert(conn, query)
# # Close the connection

# End Time
nEndTime = time.time()  # Time start
print("End time: %d seconds (epoch)" % (nEndTime), file=sys.stdout)
print("Time elapsed: %d seconds" % ((nEndTime - nStartTime)), file=sys.stdout)
