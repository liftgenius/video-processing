import json
import os
import boto3
from boto3 import Session
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


SQS_AWS_PROFILE = os.environ.get('SQS_AWS_PROFILE')
S3_AWS_PROFILE = os.environ.get('S3_AWS_PROFILE')

DEFAULT_EXPIRATION = os.environ.get('DEFAULT_EXPIRATION')

QUEUE_URL = os.environ.get('PROD_QUEUE_URL')


sqs_session = boto3.session.Session(profile_name=SQS_AWS_PROFILE)
s3_session = boto3.session.Session(profile_name=S3_AWS_PROFILE)


s3_client = s3_session.client("s3")
sqs_client = sqs_session.client("sqs")


def sqs_listen():
    message = sqs_client.receive_message(
                            QueueUrl=QUEUE_URL, 
                            MessageAttributeNames = ['All'], 
                            MaxNumberOfMessages=1
                        )
    if 'Messages' in message:
        receipt_handle = message['Messages'][0]['ReceiptHandle']
        body =  json.loads(message['Messages'][0]['Body'])
        if 'Records' in body:
            bucket_name = body["Records"][0]["s3"]["bucket"]["name"]
            object_name = body["Records"][0]["s3"]["object"]["key"]
            response = {"bucket_name": bucket_name, "object_name": object_name, "receipt_handle": receipt_handle}
            return response

    else:
        pass


def sqs_delete(receipt_handle):
    sqs_client.delete_message(QueueUrl=QUEUE_URL,ReceiptHandle=receipt_handle)
    print("deleted message: %s " % receipt_handle)


def upload_object(bucket_name, object_name, file):
    try:
        response = s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=file)
    except ClientError as e:
        return None
    
    return response


def create_presigned_post(bucket_name, object_name,
                          fields=None, conditions=None, expiration=DEFAULT_EXPIRATION):
    try:
        response = s3_client.generate_presigned_post(bucket_name,
                                                     object_name,
                                                     Fields=fields,
                                                     Conditions=conditions,
                                                     ExpiresIn=expiration)
    except ClientError as e:
        print(e)
        return None

    return response


def create_presigned_url(bucket_name, object_name, expiration=3600):
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        print(e)
        return None

    return response


def delete_object(bucket_name, object_name):
    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_name)
    
    except ClientError as e:
        print(e)
        return None
    
    return response