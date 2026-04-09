#!/usr/bin/env python
import sys, os
from classes import Queue
import json
from lib.utils.rabbitmq import get_parameters, get_plain_credentials
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

OUTGOING_EXCHANGE = os.environ.get("RABBITMQ_OUTGOING_EXCHANGE")
OUTGOING_QUEUE = os.environ.get("RABBITMQ_OUTGOING_QUEUE")


def main():
    outgoing_queue = Queue(
        parameters=get_parameters(get_plain_credentials()),
        exchange_name=OUTGOING_EXCHANGE,
        queue_name=OUTGOING_QUEUE,
        routing_key_name=OUTGOING_QUEUE,
    )

    def callback(ch, method, properties, body: str):
        try:
            data = json.loads(body)
            print(f"[R] Recieved message: {data}")
        except json.JSONDecodeError:
            print(f"Invalid JSON string: {body}")

    outgoing_queue.channel.basic_qos(prefetch_count=1)
    outgoing_queue.channel.basic_consume(
        queue=outgoing_queue.queue_name, on_message_callback=callback, auto_ack=True
    )

    print(" [*] Waiting for outgoing messages. To exit press CTRL+C")
    outgoing_queue.channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
