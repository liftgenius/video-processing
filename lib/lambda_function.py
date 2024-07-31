import cv2
import os
import requests
import json
from lib import boto3_utils
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# LAMBDA_URL = os.environ.get('LAMBDA_URL')
LAMBDA_URL = "http://127.0.0.1:3000/detect_bar"


def convert_nd_array_to_image(frame):
    ok, image =  cv2.imencode('.jpg', frame)
    if ok:
        return image.tobytes()
    else:
        print("cannot convert to image")
        return None


def invoke_lambda(presigned_url):
    try:
        response = requests.post(LAMBDA_URL, json={"url": presigned_url})
        data = response.json()
        print(f" λ {data}")
        if "pred_boxes" in data and len(data["pred_boxes"]) > 0:
            return data
        else:
            return -1

    except:
        print("LAMBDA ERROR!")
        return -1



def get_inference(frame, name):

    file = convert_nd_array_to_image(frame)
    # bucket_name = os.environ.get('PROD_IMAGE_BUCKET')
    bucket_name = "image-uploads"
    object_name = name + ".jpg"

    if file != None:

        uploaded_object = boto3_utils.upload_object(bucket_name, object_name, file)
        presigned_url = boto3_utils.create_presigned_url(bucket_name, object_name)
        # FOR USE WHEN USING LOCALSTACK S3 with SAM LAMBDA
        # REPLACE THE HOSTNAME WITH THE IP OF THE DOCKER CONTAINER RUNNING LOCALSTACK
        # docker inspect localstack-main
        edited_url = str(presigned_url).replace("localhost.localstack.cloud", "172.17.0.2")
        # inference = invoke_lambda(presigned_url)
        inference = invoke_lambda(edited_url)
        boto3_utils.delete_object(bucket_name=bucket_name, object_name=object_name)
        return inference


    

