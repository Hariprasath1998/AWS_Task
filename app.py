import logging
import boto3
import os
from boto3 import session
from botocore.exceptions import ClientError
from datetime import datetime


def create_bucket(bucket_name, region=None):

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)

            s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# print(srcBucket,'created',create_bucket(srcBucket))
# print(dstBucket,'created',create_bucket(dstBucket))

def encrypt_bucket(bucketName):
    s3_client = boto3.client('s3')
    s3_client.put_bucket_encryption(
        Bucket=bucketName,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                },
            ]
        }
    )
# encrypt_bucket(srcBucket)
# encrypt_bucket(dstBucket)

def versionBucket(bucketName):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(srcBucketString)

    try:
        bucket.Versioning().enable()
        print("Enabled versioning on bucket {}.".format( bucket.name))
    except ClientError:
        print("Couldn't enable versioning on bucket {}.".format( bucket.name))
        raise

def setLifecyclePolicy(bucketName, expiration = 7):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(srcBucketString)
    try:
        bucket.LifecycleConfiguration().put(
            LifecycleConfiguration={
                'Rules': [{
                    'Status': 'Enabled',
                    'Prefix': '',
                    'NoncurrentVersionExpiration': {'NoncurrentDays': expiration}
                }]
            }
        )
        print("Configured lifecycle to expire noncurrent versions after {} days "
                    "on bucket {}.".format( expiration, bucket.name))
    except ClientError as error:
        print("Couldn't configure lifecycle on bucket {} because {}. "
                       "Continuing anyway.".format(bucket.name, error))

def upload_file(file_name, bucketName, object_name=None):

    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file('./Hello.txt', bucketName, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



srcBucketString = "hariprasath-src-bucket-19989"
dstBucketString = "hariprasath-dst-bucket-19989"

s3_client = boto3.client('s3')
dynamoDbClient = boto3.client('dynamodb')

# s3_session = boto3.Session()
# s3_resource = s3_session.resource('s3')
s3_resource = boto3.resource('s3')


# srcBucket = s3_resource.Bucket(srcBucketString)
# dstBucket = s3_resource.Bucket(dstBucketString)
# upload_file('Hello.txt', srcBucketString)

# dstBucket = s3_resource.Bucket(dstBucketString)

# source = {'Bucket': srcBucketString, 'Key': 'Hello.txt'}

# dstBucket.copy(source, 'Hello.txt')










def getMetadata(bucketName, key):

    try:
        metadata = s3_client.head_object(Bucket=srcBucketString, Key='Hello.txt')
    except:
        print("Failed {}".format('Hello.txt'))

    objectRow = {
        'Object Name': key,
        'Last Modified': datetime.timestamp(metadata['LastModified']),
        'Size': metadata['ContentLength'],
        'SSE': metadata['ServerSideEncryption']
    }
    # print(objectRow)
    print(datetime.timestamp(metadata['LastModified']))

# getMetadata(srcBucketString, 'Hello.txt')


table = dynamoDbClient.create_table(
        TableName='ObjectMetadata',
        KeySchema=[
            {
                'AttributeName': 'Name',
                'KeyType': 'HASH'  
            },
            {
                'AttributeName': 'LastModified',
                'KeyType': 'RANGE'  
            },
            {
                'AttributeName': 'Size',
                'KeyType': 'RANGE'
            },
            {
                'AttributeName': 'SSE',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'LastModified',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'Size',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'SSE',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )