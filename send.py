#!/usr/bin/env python
import os
from uuid import uuid4
from lib.utils.rabbitmq import get_parameters, get_plain_credentials
from lib.utils.s3 import get_objects
from dotenv import load_dotenv, find_dotenv
import json
from classes import Queue

load_dotenv(find_dotenv())

INCOMING_EXCHANGE = os.environ.get("RABBITMQ_INCOMING_EXCHANGE")
INCOMING_QUEUE = os.environ.get("RABBITMQ_INCOMING_QUEUE")

BUCKET_NAME = os.environ.get("VIDEO_UPLOAD_BUCKET_NAME", "video-uploads")


# awslocal s3api put-object --bucket video-uploads --key deadlift_test.mov --body ~/Downloads/deadlift_test.mov


def main():
    incoming_queue = Queue(
        parameters=get_parameters(credentials=get_plain_credentials()),
        exchange_name=INCOMING_EXCHANGE,
        queue_name=INCOMING_QUEUE,
        routing_key_name=INCOMING_QUEUE,
    )

    for obj in get_objects(bucket_name=BUCKET_NAME):
        obj_name = obj.get("Key")
        if obj_name and obj_name.endswith(".mov"):
            payload = {"bucket": BUCKET_NAME, "key": obj_name, "job_id": str(uuid4())}
            incoming_queue.send_message(message=json.dumps(payload))


if __name__ == "__main__":
    main()
