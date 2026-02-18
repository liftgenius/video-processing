import functools
import functools, time, threading, json
import os
from lib import track_bar, boto3_utils, rabbitmq_utils
from dotenv import load_dotenv, find_dotenv

env_loaded = load_dotenv(find_dotenv())
print(f"Successfully loaded .env: {env_loaded}")
# exit(0)

# SQS_AWS_PROFILE = os.environ.get('SQS_AWS_PROFILE')
# S3_AWS_PROFILE = os.environ.get('S3_AWS_PROFILE')

RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_OUTGOING_EXCHANGE = os.environ.get('RABBITMQ_OUTGOING_EXCHANGE')
RABBITMQ_OUTGOING_QUEUE = os.environ.get('RABBITMQ_OUTGOING_QUEUE')
RABBITMQ_INCOMING_EXCHANGE = os.environ.get('RABBITMQ_INCOMING_EXCHANGE')
RABBITMQ_INCOMING_QUEUE = os.environ.get('RABBITMQ_INCOMING_QUEUE')
# DEFAULT_EXPIRATION = os.environ.get('DEFAULT_EXPIRATION')



def process_video(conn, ch, delivery_tag, data):
    bucket = data["bucket"]
    key = data["key"]
    job_id = data["job_id"]
    # thread_id = threading.get_ident()
    message = json.dumps({"job_id": job_id, "status": "analyzing"})
    rabbitmq_utils.send_message(
        exchange_name=RABBITMQ_OUTGOING_EXCHANGE,
        queue_name=RABBITMQ_OUTGOING_QUEUE,
        routing_key_name=RABBITMQ_OUTGOING_QUEUE, 
        message=message
    )
    url = boto3_utils.create_presigned_url(bucket_name=bucket, object_name=key)
    print(f" \n 🗓️ {time.strftime('%l:%M:%S%p %Z on %b %d, %Y')}")
    print(f" 💼 Job: {job_id}")
    print(f" 🎥 Processing video...")
    output_url = track_bar.analyze(RABBITMQ_OUTGOING_EXCHANGE, RABBITMQ_OUTGOING_QUEUE, job_id, url)
    if output_url == -1:
        message = json.dumps({"job_id": job_id, "status": "failure"})
        print(" ❌ Could not process video\n")
        rabbitmq_utils.send_message(
            exchange_name=RABBITMQ_OUTGOING_EXCHANGE,
            queue_name=RABBITMQ_OUTGOING_QUEUE,
            routing_key_name=RABBITMQ_OUTGOING_QUEUE, 
            message=message
        )
        # print(' \n 📧 Waiting for messages. To exit press CTRL+C')
        cb = functools.partial(rabbitmq_utils.ack_message, ch, delivery_tag)
        conn.add_callback_threadsafe(cb)
    else:
        print(" 🗑️ Deleting source video")
        boto3_utils.delete_object(bucket_name=bucket, object_name=key)
        print(" ✅ Done!")
        message = json.dumps(
            {
                "job_id": job_id,
                "status": "success",
                "artifact": output_url
            }
        )
        rabbitmq_utils.send_message(
            exchange_name=RABBITMQ_OUTGOING_EXCHANGE,
            queue_name=RABBITMQ_OUTGOING_QUEUE,
            routing_key_name=RABBITMQ_OUTGOING_QUEUE, 
            message=message
        )
        cb = functools.partial(rabbitmq_utils.ack_message, ch, delivery_tag)
        conn.add_callback_threadsafe(cb)


def on_message(ch, method_frame, _header_frame, body, args):
    (conn, thrds) = args
    delivery_tag = method_frame.delivery_tag
    payload = body.decode('utf8')
    try:
        data = json.loads(payload)
        if data["bucket"] and data["key"] and data["job_id"]:
            t = threading.Thread(
                target=process_video, 
                args=(conn, ch, delivery_tag, data)
            )
            t.start()
            thrds.append(t)
        else:
            print(f" ❌ Invalid data:\n{data}")
            cb = functools.partial(rabbitmq_utils.ack_message, ch, delivery_tag)
            conn.add_callback_threadsafe(cb)  
    except:
        print(f" ❌ Did not recognize message:\n{data}")
        cb = functools.partial(rabbitmq_utils.ack_message, ch, delivery_tag)
        conn.add_callback_threadsafe(cb)


credentials = rabbitmq_utils.get_plain_credentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
parameters = rabbitmq_utils.get_parameters(credentials=credentials)
threads = []

consumer_connection, consumer_channel = \
    rabbitmq_utils.setup_channel(
        parameters=parameters, 
        exchange_name=RABBITMQ_INCOMING_EXCHANGE, 
        queue_name=RABBITMQ_INCOMING_QUEUE, 
        routing_key_name=RABBITMQ_INCOMING_QUEUE
    )

on_message_callback = functools.partial(on_message, args=(consumer_connection, threads))
consumer_channel.basic_qos(prefetch_count=1)
consumer_channel.basic_consume(RABBITMQ_INCOMING_QUEUE, on_message_callback)
try:
    consumer_channel.start_consuming()
    print(' \n 📧 Waiting for messages. To exit press CTRL+C')
except KeyboardInterrupt:
    consumer_channel.stop_consuming()

# Wait for all to complete
for thread in threads:
    thread.join()

consumer_connection.close()