from gettext import find
from lib import boto3_utils, track_bar


if __name__ == "__main__":

    while True:

        job = boto3_utils.sqs_listen()
        if job:
            if "bucket_name" in job and "object_name" in job and "receipt_handle" in job:
                print("starting job: %s" % job["receipt_handle"])
                presigned_url = boto3_utils.create_presigned_url(job["bucket_name"], job["object_name"])
                output_url = track_bar.analyze(presigned_url)
                print("Output Video: %s" % output_url)

                boto3_utils.sqs_delete(job["receipt_handle"])
                boto3_utils.delete_object(job["bucket_name"], job["object_name"])

            