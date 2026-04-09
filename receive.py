#!/usr/bin/env python
import sys, os
from tasks import process_video, convert_avi_to_webm, on_success, on_error
from classes import Queue, Job
import json
from celery import chain
from lib.utils.rabbitmq import get_parameters, get_plain_credentials
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

OUTGOING_EXCHANGE = os.environ.get("RABBITMQ_OUTGOING_EXCHANGE")
OUTGOING_QUEUE = os.environ.get("RABBITMQ_OUTGOING_QUEUE")
INCOMING_EXCHANGE = os.environ.get("RABBITMQ_INCOMING_EXCHANGE")
INCOMING_QUEUE = os.environ.get("RABBITMQ_INCOMING_QUEUE")


def _consume(data: dict) -> None:
    job = Job(**data)
    print(f"[Q] Queuing job: {str(job)}")
    context = job.to_dict() | {"exchange": OUTGOING_EXCHANGE, "queue": OUTGOING_QUEUE}
    pipeline = chain(process_video.s(context), convert_avi_to_webm.s())
    pipeline.apply_async(link=on_success.s(context), link_error=on_error.s(context))


def main():
    incoming_queue = Queue(
        parameters=get_parameters(get_plain_credentials()),
        exchange_name=INCOMING_EXCHANGE,
        queue_name=INCOMING_QUEUE,
        routing_key_name=INCOMING_QUEUE,
    )

    def callback(ch, method, properties, body: str):
        try:
            data = json.loads(body)
            print(f"[R] Recieved message: {body}")
            _consume(data)
        except json.JSONDecodeError:
            print(f"Invalid JSON string: {body}")

    incoming_queue.channel.basic_qos(prefetch_count=1)
    incoming_queue.channel.basic_consume(
        queue=incoming_queue.queue_name, on_message_callback=callback, auto_ack=True
    )

    print(" [*] Waiting for incoming messages. To exit press CTRL+C")
    incoming_queue.channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
