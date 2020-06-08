import boto3
import os

# Generate clients to access data
dx = boto3.client('dataexchange',  region_name='us-east-1')
s3 = boto3.client('s3')

# Get the
# Keys: ['Arn', 'AssetDetails', 'AssetType', 'CreatedAt', 'DataSetId', 'Id', 'Name', 'RevisionId', 'UpdatedAt']
# AssetDetails has a nested dict like: {'S3SnapshotAsset': {'Size': 624028320}}
def getAllRevisionAssets(datasetid, revisionid):
    assetsList = []

    revisionDict = dx.list_revision_assets(
        DataSetId=datasetid,
        RevisionId=revisionid
    )
    nextToken = revisionDict.get('NextToken')

    assetsList += revisionDict.get('Assets')
    while nextToken:
        revisionDict = dx.list_revision_assets(
            DataSetId=datasetid,
            RevisionId=revisionid,
            NextToken=nextToken)
        assetsList += revisionDict.get('Assets')
        nextToken = revisionDict.get('NextToken')

    return assetsList