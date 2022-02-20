import cv2
import os
import requests
import json
from lib import boto3_utils
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

LAMBDA_URL = os.environ.get('LAMBDA_URL')


def convert_nd_array_to_image(frame):
    ok, image =  cv2.imencode('.jpg', frame)
    if ok:
        return image.tobytes()
    else:
        print("cannot convert to image")
        return None


def invoke_lambda(presigned_url):

    response = requests.post(LAMBDA_URL, json={"url": presigned_url})
    data = response.json()
    if "body" in data:
        return json.loads(data["body"])
    else:
        print(response.json)


def get_inference(frame, name):

    file = convert_nd_array_to_image(frame)
    bucket_name = os.environ.get('PROD_IMAGE_BUCKET')
    object_name = name + ".jpg"

    if file != None:

        uploaded_object = boto3_utils.upload_object(bucket_name, object_name, file)
        presigned_url = boto3_utils.create_presigned_url(bucket_name, object_name)

        inference = invoke_lambda(presigned_url)
        boto3_utils.delete_object(bucket_name=bucket_name, object_name=object_name)

        return inference


    

