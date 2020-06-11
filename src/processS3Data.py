'''
Insight Data Engineering Project
Version 0.0.01

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Takes S3 Buckets for processing and outputs the results

Columns:
'''

# IMPORTS
import os
import boto3
import csv

import pandas as pd

# FUNCTIONS
def grabFileFromS3(s3Bucket):
    # Create a client for s3 in boto3
    s3 = boto3.client('s3')

# BUCKET INFORMATION
bucketId = 'ssd-package-s3-dev'
prefixId = 'ams/2020/'
outputLoc = '/data/dev/'

# PROCESSING
# Open a connection to
s3r = boto3.resource('s3')

# Tell me what my buckets are
for bucket in s3r.buckets.all():
    print(bucket.name)

# iterate through buckets
s3 = boto3.client('s3')
response = s3.list_objects_v2(
        Bucket=bucketId,
        Prefix=prefixId
)

# Through the objects
for s3_obj in response['Contents']:
    obj = s3.get_object(Bucket=bucketId, Key=s3_obj['Key'])
    # Do your converting, and uploading here
    # object 'Body' is streaming
    df = pd.read_csv(obj['Body'])
    header = df.head()
    print(header)

    outputFileName = '{}{}.parquet'.format(outputLoc, s3_obj['Key'])
    outputDirLoc = os.path.dirname(outputFileName)

    # Make any directories that don't exist
    print(outputDirLoc)
    if not os.path.exists(outputDirLoc):
        try:
            os.makedirs(outputDirLoc)
            print('Making directories: {}'.format(outputDirLoc))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            print('Directories already exist.')

    # Writes out the Parquet File
    df.to_parquet(outputFileName)

    # Access the Parquet file
    pq = pd.read_parquet(outputFileName, engine='pyarrow')
    header = pq.head()
    print(header)

    print('Output completed: {}'.format(outputFileName))


