'''
Insight Data Engineering Project
Version 0.0.01

Contact:
Edmund Young
dryoung@solidstate.dev

Purpose:
Move Automated Manifest System files to S3 Bucket

Columns:
'''

import io
import boto3
import time
import json
#import ssdpackagefunctions as fn
import mvDataParseFunctions as fn

# Timer
nStartTime = time.time()   # Time start

# Open S3 file

# Try to find the file
# Exit out if the file can't be found

# Lists:
tmpList = []

print('hello')
# Create Client connections
dx = boto3.client('dataexchange', region_name='us-east-1')
#s3 = boto3.client('s3')
s3 = boto3.resource('s3')

# Define IDs to use
dataSetId  = 'd0e9bd6148e8f14889980954017b0927'
revisionId = '9f8ccb4b471f642909582c248d65afeb'

# Connect to Data Exchange
response = dx.get_asset(
    DataSetId=dataSetId,
    RevisionId=revisionId,
    AssetId='e0625157f2be4fadb77a064202f56e42'
)

# returns a Dict of the Data Set
response = dx.get_data_set(DataSetId=dataSetId)
# type(response)

#print(list(response.keys()))
#print(list(response.values()))
print(response.get('UpdatedAt'))
#print(response.get('Arn'))
#print(response.get('AssetType'))
print(response.get('CreatedAt'))
#print(response.get('Description'))
print(response.get('Id'))
#print(response.get('Name'))
#print(response.get('Origin'))
#print(response.get('OriginDetails'))
#print(response)

# Paginator
paginator = dx.get_paginator('list_data_set_revisions')

responseIterator = paginator.paginate(
    DataSetId=dataSetId,
    PaginationConfig={
        'MaxItems': 123,
        'PageSize': 123
    }
)

# Go through the responses
for page in responseIterator:
    revisionsList = page.get('Revisions')
    # Sort the revision dictionary
    revisionsList = sorted(revisionsList, key=lambda k: k['UpdatedAt'], reverse=True)

    for row in revisionsList:
        #print(row.keys())
        print(row.get('UpdatedAt'))
        print(row.get('DataSetId'))
        print(row.get('Id'))
        print(row.get('Finalized'))

# Returns a list of asset dictionaries
assetList = fn.getAllRevisionAssets(dataSetId,revisionId)
assetList = sorted(assetList, key=lambda k: k['UpdatedAt'], reverse=True)
# Keys: ['Arn', 'AssetDetails', 'AssetType', 'CreatedAt', 'DataSetId', 'Id', 'Name', 'RevisionId', 'UpdatedAt']
# AssetDetails has a nested dict like: {'S3SnapshotAsset': {'Size': 624028320}}
for row in assetList:
    #print(row.keys())
    print(row.get('UpdatedAt'))
    print(row.get('DataSetId'))
    print(row.get('RevisionId'))
    print(row.get('Id'))
    print(row.get('Name'))
    print(row.get('AssetType'))
    print(row.get('AssetDetails'))
    print(row.get('Arn'))
    print('\n')

revisionList = dx.list_revision_assets(
        DataSetId=dataSetId,
        RevisionId=revisionId
    )
#print(revisionList.keys())
print('\n')

# Print out the bucket names
for bucket in s3.buckets.all():
    print(bucket.name)