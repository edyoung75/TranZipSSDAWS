'''
Insight Data Engineering Project
Version 0.0.6

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Reads the parquet files and updates the data warehouse


'''
# IMPORTS:
import pandas as pd
import awswrangler as wr
import sqlalchemy
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
fileHeaderList = [
    's3://ssd-test-dev/spark/header/All'
]

fileConsigneeList = [
    's3://ssd-test-dev/spark/consignee/All'
]

fileCargoDescList = [
    's3://ssd-test-dev/spark/cargodesc/All'
]

fileContainerList = [
    's3://ssd-test-dev/spark/container/All'
]

fileHazmatList = [
    's3://ssd-test-dev/spark/hazmat/All'
]

fileHazmatClassList = [
    's3://ssd-test-dev/spark/hazmatclass/All'
]

fileMarksNumbersList = [
    's3://ssd-test-dev/spark/marksnumbers/All'
]

fileNotifyPartyList = [
    's3://ssd-test-dev/spark/notifyparty/All'
]

fileShipperList = [
    's3://ssd-test-dev/spark/shipper/All'
]

fileTariffList = [
    's3://ssd-test-dev/spark/tariff/All'
]

filebillgenList = [
    's3://ssd-test-dev/spark/billgen/All'
]

filesummonthweightList = [
    's3://ssd-test-dev/spark/summonthweight/All'
]

filesumportweightList = [
    's3://ssd-test-dev/spark/sumportweight/All'
]

filesumconsigneeweightList = [
    's3://ssd-test-dev/spark/sumconsigneeweight/All'
]

# FUNCTIONS:
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
dfbillgen = getParquetConcatenate(filebillgenList)
psqlTableNamebillgen = 'billgen'
dfsummonthweight = getParquetConcatenate(filesummonthweightList)
psqlTableNamesummonthweight = 'summonthweight'
dfsumportweight = getParquetConcatenate(filesumportweightList)
psqlTableNamesumportweight = 'sumportweight'
dfsumconsigneeweight = getParquetConcatenate(filesumconsigneeweightList)
psqlTableNamesumconsigneeweight = 'sumconsigneeweight'

# Create Connections
uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(username,password,host,port,dbase)
alchemyEngine = sqlalchemy.create_engine(uri, pool_recycle=60)
psqlConnection = alchemyEngine.connect()

# Try to upload everything
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
    dfbillgen.to_sql(psqlTableNamebillgen, psqlConnection, if_exists='replace')
    dfsummonthweight.to_sql(psqlTableNamesummonthweight, psqlConnection, if_exists='replace')
    dfsumportweight.to_sql(psqlTableNamesumportweight, psqlConnection, if_exists='replace')
    dfsumconsigneeweight.to_sql(psqlTableNamesumconsigneeweight, psqlConnection, if_exists='replace')
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
    print("PostgreSQL Table %s has been created successfully." % psqlTableNamebillgen)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNamesummonthweight)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNamesumportweight)
    print("PostgreSQL Table %s has been created successfully." % psqlTableNamesumconsigneeweight)
finally:
    psqlConnection.close()

# End Time
nEndTime = time.time()  # Time start
print("End time: %d seconds (epoch)" % (nEndTime), file=sys.stdout)
print("Time elapsed: %d seconds" % ((nEndTime - nStartTime)), file=sys.stdout)
