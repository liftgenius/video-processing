# from gettext import find
from lib import track_bar, boto3_utils
import pika, sys, os, time, json

def main():
    video_queue = "video_processing_queue"
    analyzed_queue = "analyzed_videos_queue"
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='0.0.0.0'))
    channel = connection.channel()

    channel.queue_declare(queue=video_queue, durable=True)
    channel.queue_declare(queue=analyzed_queue, durable=True)

    def process(bucket, key, uuid):
        url = boto3_utils.create_presigned_url(bucket_name=bucket, object_name=key)
        print(f" \n 🗓️ {time.strftime('%l:%M:%S%p %Z on %b %d, %Y')}")
        print(f" 💼 Job: {uuid}")
        print(f" 🎥 Processing video...")
        
        output_url = track_bar.analyze(url)
        connection.close()

        if output_url == -1:
            message = json.dumps(
                {
                    "uuid": uuid,
                    "status": "fail"
                }
            )
            print(" ❌ Could not process video\n")
            # channel.basic_publish(exchange='', routing_key=analyzed_queue, body=message)
            print(f" ✈️ Sent {message}")
            print(' \n 📧 Waiting for messages. To exit press CTRL+C')
        else:
            print(" 🗑️ Deleting source video")
            boto3_utils.delete_object(bucket_name=bucket, object_name=key)
            print(" ✅ Done!")
            message = json.dumps(
                {
                    "uuid": uuid,
                    "status": "success",
                    "url": output_url
                }
            )
            # channel.basic_publish(exchange='', routing_key=analyzed_queue, body=message)
            print(f" ✈️ Sent {message}")
            # connection.close()
            print(' \n 📧 Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        payload = body.decode('utf8')
        try:
            data = json.loads(payload)
            if data["bucket"] and data["key"] and data["uuid"]:
                process(data["bucket"], data["key"], data["uuid"])
            else:
                print(f" ❌ Invalid data:\n{data}")   
        except:
            print(f" ❌ Did not recognize message:\n{data}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='video_processing_queue', on_message_callback=callback)

    print(' 📧 Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

            