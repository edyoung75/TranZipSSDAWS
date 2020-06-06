import io
import boto3
import time
import json
import ssdpackagefunctions as fn


# Timer
nStartTime = time.time()   # Time start

# Open S3 file

# Try to find the file
# Exit out if the file can't be found

# Lists:
tmpList = []


# Open a connect a connection to S3
s3 = boto3.resource('s3')

# Connect to Data Exchange
client = boto3.client('dataexchange')
response = client.get_asset(
    DataSetId='d0e9bd6148e8f14889980954017b0927',
    RevisionId='9f8ccb4b471f642909582c248d65afeb',
    AssetId='e0625157f2be4fadb77a064202f56e42'
)

# returns a Dict of the Data Set
response = client.get_data_set(DataSetId='d0e9bd6148e8f14889980954017b0927')
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
paginator = client.get_paginator('list_data_set_revisions')

responseIterator = paginator.paginate(
    DataSetId='d0e9bd6148e8f14889980954017b0927',
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
        print(row.keys())
        print(row.get('UpdatedAt'))
        print(row.get('DataSetId'))
        print(row.get('Id'))
        print(row.get('Finalized'))
        print('\n')

# Print out the bucket names
for bucket in s3.buckets.all():
    print(bucket.name)