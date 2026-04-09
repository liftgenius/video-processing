import os
import boto3
from typing import IO
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

S3_ENDPOINT = os.getenv("LOCALSTACK_HOST", "localhost.localstack.cloud")
S3_PORT = os.getenv("LOCALSTACK_PORT", "4566")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_ACCESS_SECRET_KEY = os.getenv("AWS_ACCESS_SECRET_KEY", "test")


session = boto3.Session()
s3_client = session.client(
    service_name="s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_ACCESS_SECRET_KEY,
    endpoint_url=f"http://{S3_ENDPOINT}:{S3_PORT}",
)


def get_objects(bucket_name: str):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    objects = response.get("Contents", [])
    return objects


def upload_object(bucket_name: str, object_name: str, file: Path) -> dict:
    try:
        response = s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=file)
        return response
    except ClientError as e:
        raise ClientError(error_response=f"Error {e} while uploading object to S3")


def upload_file(bucket_name: str, object_name: str, file: Path) -> dict:
    try:
        response = s3_client.upload_file(str(file), bucket_name, object_name)
        return response
    except ClientError as e:
        raise ClientError(error_response=f"Error {e} while uploading file to S3")


def upload_stream(bucket_name: str, object_name: str, parts: IO[bytes]):
    s3_client.upload_fileobj(parts, bucket_name, object_name)


def create_presigned_url(bucket_name: str, object_name: str, expiration=3600) -> str:
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
        return response
    except ClientError as e:
        raise ClientError(
            error_response=f"Error {e} while generating presigned URL for {object_name}"
        )


def delete_object(bucket_name: str, object_name: str) -> dict:
    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_name)

    except ClientError as e:
        raise ClientError(error_response=f"Error {e} while deleting {object_name}")

    return response
