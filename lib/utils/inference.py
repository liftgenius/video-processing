import cv2
import os
import requests
from lib.utils import s3
from dotenv import load_dotenv, find_dotenv
import logging

load_dotenv(find_dotenv())

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
LAMBDA_INFERENCE_BUCKET_NAME = os.environ.get("LAMBDA_INFERENCE_BUCKET_NAME")
LAMBDA_INFERENCE_URL = os.environ.get("LAMBDA_INFERENCE_URL")
LOCALSTACK_HOST = os.environ.get("LOCALSTACK_HOST")
LOCALSTACK_IP = os.environ.get("LOCALSTACK_IP")


def convert_nd_array_to_image(frame: cv2.typing.MatLike):
    ok, image = cv2.imencode(".jpg", frame)
    if ok:
        return image.tobytes()
    else:
        logging.error("cannot convert to image")
        return None


def invoke_lambda(presigned_url: str) -> tuple[bool, dict]:
    try:
        response = requests.post(LAMBDA_INFERENCE_URL, json={"url": presigned_url})
        data = response.json()
        if "pred_boxes" in data and len(data["pred_boxes"]) > 0:
            return (True, data)
        else:
            return (False, {})

    except:
        logging.error("LAMBDA ERROR!")
        return (False, {})


def get_inference(frame: cv2.typing.MatLike, name: str) -> tuple[bool, dict]:

    file = convert_nd_array_to_image(frame)
    bucket_name = LAMBDA_INFERENCE_BUCKET_NAME
    object_name = f"{name}.jpg"

    if file != None:
        _uploaded_object = s3.upload_object(bucket_name, object_name, file)
        presigned_url = s3.create_presigned_url(bucket_name, object_name)
        # WHEN USING LOCALSTACK S3 with SAM LAMBDA
        # REPLACE THE HOSTNAME WITH THE IP OF THE DOCKER CONTAINER RUNNING LOCALSTACK
        # docker inspect localstack-main
        if ENVIRONMENT == "local":
            presigned_url = str(presigned_url).replace(LOCALSTACK_HOST, LOCALSTACK_IP)
        ok, inference = invoke_lambda(presigned_url)
        s3.delete_object(bucket_name=bucket_name, object_name=object_name)
        return (ok, inference)
    else:
        return (False, {})
