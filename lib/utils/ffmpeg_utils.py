import re
import subprocess
from lib.utils import s3


def parse_ffmpeg_frame(line):
    data = re.split("(frame=\s*)(\d{1,})", line)
    try:
        return data[2]
    except:
        return -1


def get_ffmpeg_progress(frame_number, total_frames):
    if int(frame_number) > 0 and int(total_frames) > 0:
        return (round(int(frame_number) / int(total_frames), 3)) * 100.0
    else:
        return -1


def stream_ffmpeg_to_s3(input_url: str, bucket_name: str, object_name: str):
    command = [
        "ffmpeg",
        "-i",
        input_url,
        "-c:v",
        "libvpx-vp9",
        "-crf",
        "30",
        "-b:v",
        "0",
        "-deadline",
        "realtime",  # Reduces latency/processing time
        "-cpu-used",
        "8",  # Max speed (0=slowest, 8=fastest)
        "-c:a",
        "libopus",
        "-f",
        "webm",
        "pipe:1",
    ]

    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=10**6
    )

    try:
        s3.upload_stream(
            bucket_name=bucket_name, object_name=object_name, parts=process.stdout
        )

        process.wait()
        if process.returncode != 0:
            error = process.stderr.read().decode()
            raise Exception(f"FFmpeg Error: {error}")

    except Exception as e:
        process.kill()
        raise e
