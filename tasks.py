import cv2
import os
from celery import Celery
import subprocess as sp
from classes import Job, Status, Queue, VideoExtension
from lib.tracking.main import analyze
from lib.utils.s3 import create_presigned_url, upload_file
from lib.utils.rabbitmq import get_parameters, get_plain_credentials
from lib.utils.ffmpeg_utils import (
    parse_ffmpeg_frame,
    get_ffmpeg_progress,
    stream_ffmpeg_to_s3,
)
from dotenv import load_dotenv, find_dotenv
import logging
import json

load_dotenv(find_dotenv())

RABBITMQ_USERNAME = os.environ.get("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")

RABBITMQ = f"{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}//"

UPLOAD_BUCKET = os.environ.get("VIDEO_UPLOAD_BUCKET_NAME", "video-uploads")

app = Celery("video-processing", broker=f"amqp://{RABBITMQ}", backend="rpc://")

app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1)


def create_queue_from_ctx(context: dict):
    return Queue(
        parameters=get_parameters(credentials=get_plain_credentials()),
        exchange_name=context["exchange"],
        queue_name=context["queue"],
        routing_key_name=context["queue"],
    )


@app.task(bind=True)
def process_video(self, context: dict) -> dict:

    outgoing_queue = create_queue_from_ctx(context)

    job = Job(**context)
    presigned_url = create_presigned_url(bucket_name=job.bucket, object_name=job.key)
    logging.info(f"[{str(job)}] URL: {presigned_url}")
    cap = cv2.VideoCapture(presigned_url)
    if not cap.isOpened():
        raise Exception("Could not open video file.")
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    filepath = analyze(
        task=self, cap=cap, total_frames=total_frames, job=job, queue=outgoing_queue
    )
    cap.release()
    outgoing_queue.close()

    upload_file(bucket_name=job.bucket, object_name=job.job_id, file=filepath)
    presigned_url = create_presigned_url(bucket_name=job.bucket, object_name=job.job_id)
    job.update(status=Status.SUCCESS, artifact=presigned_url)
    return job.to_dict() | context

    return result, total_frames


@app.task()
def convert_avi_to_webm(context: dict):
    job = Job(**context)
    print(job.artifact)
    # outgoing_queue = create_queue_from_ctx(context)
    stream_ffmpeg_to_s3(
        input_url=job.artifact,
        bucket_name=job.bucket,
        object_name=f"{job.job_id}{VideoExtension.WEBM.value}",
    )
    # print(input)
    # if None not in (job_id, key, input):
    # output = f"/home/jdn1995/Videos/{job_id}{VideoExtension.WEBM.value}"
    # command = f"ffmpeg -i {input} -c:v libsvtav1 -preset 4 -crf 30 -g 240 -pix_fmt yuv420p10le -svtav1-params tune=0:film-grain=8 -c:a -b:a -y {output}"
    # command = [
    #         "ffmpeg",
    #         "-y",
    #         "-i",
    #         input,
    #         "-c:v",
    #         "libvpx-vp9",
    #         "-crf",
    #         "30",
    #         "-b:v",
    #         "0",
    #         "-c:a",
    #         "libopus",
    #         output,
    #     ]

    # try:
    #         # subprocess.run(command, check=True, capture_output=True)
    #     process = sp.Popen(
    #         command,
    #         stdout=None,
    #         stderr=sp.PIPE,
    #         universal_newlines=True,
    #         shell=True,
    #     )
    #     for line in process.stderr:
    #         frame_number = parse_ffmpeg_frame(line)
    #         progress = get_ffmpeg_progress(frame_number, total_frames)
    #         print(progress)
    #             # if progress % 5 < 0.0001 and progress != last_progress:
    #             #     self.update_state(
    #             #         task_id=f"{job_id}_{key}",
    #             #         state="PROGRESS",
    #             #         meta={"progress": progress},
    #             #     )
    #             #     message = {"job_id": job_id, "progress": f"{progress:.0f}"}
    #             #     outgoing_queue.send_message(message=json.dumps(message))
    #             #     last_progress = progress

    #     # return f"Successfully converted {input} to {output}"
    # except sp.CalledProcessError as e:
    #     return f"Failed to convert {input}: {e.stderr.decode()}"


@app.task
def on_success(result: dict, context):
    logging.info(result)
    outgoing_queue = create_queue_from_ctx(context)
    outgoing_queue.send_message(json.dumps(result))
    outgoing_queue.close()


@app.task
def on_error(request, exc, traceback, context):
    logging.error(f"[X] Task {request.id} failed with Exception {exc}!")
    # logging.error(request)
    # print(f"[X] Task {request.id} failed.")
    # print(f"Reason: {exc}")
