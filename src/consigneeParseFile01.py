'''
Insight Data Engineering Project
Version 0.0.01

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Split csv file into separate files for processing

Columns:


CSV RAW
identifier,consignee_name,consignee_address_1,consignee_address_2,consignee_address_3,consignee_address_4,city,state_province,zip_code,country_code,contact_name,comm_number_qualifier,comm_number
202001061634,"C.H.ROBINSON INTERNATIONAL, INC.","KNOXVILLE GF I, 800 S.GAY STREET","SUITE 900, KNOXVILLE, TN 37929",TEL 865-246-6906 / FAX 865-246-6950,ATTN BEN HOLT,"","","","","","",""
202001061635,AMERICA PACIFIC CONTAINER LINE INC,"4425 ATLANTIC AVE.,SUITE A14","LONG BEACH, CA 90807",TEL 562-428-8856 FAX 562-428-2554,"","","","","","","",""
202001061637,HOMELEGANCE BY TITAN IMPORTER CO.,6045 BOAT ROCK BLVD,"ATLANTA, GA 30336 U.S.A","","","","","","","","",""
202001061638,"PRIMETAC ACQUISITION, LLC","223 GATES ROAD UNIT B,LITTLE FERRY",NJ 07643 U.S.A.,"","","","","","","","",""
202001061639,BRISKHEAT CORPORATION,4800 HILTON CORPORATE DRIVE,COLUMBUS OHIO 43232 US,"","",COLUMBUS,OH,43232,US,"","",""
202001061640,DE WELL CONTAINER SHIPPING INC.,"5553 BANDINI BLVD, UNIT A",BELL CA 90201 UNITED STATES,"","","","","","","","",""
202001061648,APEX MARITIME CO. (LAX) INC.,.,"565 BREA CANYON ROAD, SUITE D WALNU","T WALNUT, CA CA 91789 US","","","","","","","",""
202001061649,CANADIAN SOLAR (USA) INC.,3000 OAK RDSTE 400,"","","",WALNUT CREEK,CA,94597,US,"","",""
202001061650,CANADIAN SOLAR (USA) INC.,3000 OAK RDSTE 400,"","","",WALNUT CREEK,CA,94597,US,"","",""
202001061651,CANADIAN SOLAR (USA) INC.,3000 OAK RDSTE 400,"","","",WALNUT CREEK,CA,94597,US,"","",""
202001061652,VENTI AIR PRODUCTS LLC,10215 S SAM HOUSTON PKWY WSTE 200,"","","",HOUSTON,TX,77071,US,"","",""

CSV output

'''

# IMPORTS
# System arguments
import sys
import time
import os
# Manage CSV files
import csv
# Import separate functions
import ssdpackagefunctions as fn

#sys.setrecursionlimit(10000)

# Initial input: python <init file> <src file> <output file>
initFile = sys.argv[0]
#srcFileLoc = sys.argv[1]  # Location of the source file
#acceptOutputFileLoc = sys.argv[2]  # Location of the Accept It output file
#fixOutputFileLoc = sys.argv[3]  # Location of the Fix It output file

# Open S3 file
AWS = '../other/'
reader = csv.reader(AWS)  # create the csv reader
AWS_KEY = reader[1][2]
AWS_SECRET = reader[1][2]
BUCKET = 'ssd-package-'
from boto.s3.connection import S3Connection
conn = S3Connection(AWS_KEY, AWS_SECRET)
bucket = conn.get_bucket(BUCKET)
destination = bucket.new_key()
destination.name = filename
destination.set_contents_from_file(myfile)
destination.make_public()
#srcFileLoc = 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\ams_2020_202001201500_ams__cargodesc_2020__202001201500.csv'  # Location of the source file
acceptOutputFileLoc = '../output/acceptoutput.csv '  # Location of the Accept It output file
fixOutputFileLoc = '../output/fixitoutput.csv'  # Location of the Fix It output file

# Define the lists that will be used
srcData = []
tempDataList = []
tempDataList1 = []


# Error Log
errorOutputFileLoc= 'errorlog.csv'
#bError          = 0             # Error

# Timer
nStartTime      = time.time()   # Time start


# Try to find the file
# Exit out if the file can't be found
if not os.path.isfile(srcFileLoc):
    print("File path {} does not exist. Exiting...".format(srcFileLoc))
    sys.exit()
print("Finding files {} and then out putting to here {}".format(srcFileLoc, acceptOutputFileLoc))


# Go through the file line by line and create a dictionary of each unique item
with open(srcFileLoc) as srcFile:
    reader = csv.reader(srcFile)  # create the csv reader
    srcDataHeader = next(reader)  # grab the header

    tempDataReaderList = []

    # Converting reader into a list and into memory
    for row in reader:
        tempDataReaderList.append(row)
    print('The file started with {} data points.'.format(len(tempDataReaderList)))


    # Finalized Data Set
    # Accepted List, fix it
    tempDataList, tempDataList1 = fn.fLenComp(tempDataReaderList,srcDataHeader)

# Open the output file and overwrite the existing data with the new data
# Error handling non-integer cost values and output an error log
#fn.printOutput(errorOutputFile, aErrors)  # Create Error Log
fn.printOutput(acceptOutputFileLoc, tempDataList)       # Create Aggregated Dataset
fn.printOutput(fixOutputFileLoc, tempDataList1)         # Create Aggregated Dataset

nEndTime = time.time()   # Time end

print("End time: %d seconds (epoch)" % (nEndTime))
print("Time elapsed: %d seconds" % ((nEndTime-nStartTime)))
