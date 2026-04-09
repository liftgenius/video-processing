import cv2
import os
from pathlib import Path
from celery import Task

from dotenv import load_dotenv, find_dotenv
from lib.utils.inference import get_inference

import lib.tracking.utils as utils
from classes import VideoExtension, TrackMode, TrackData, Job, Queue
import logging
import json

load_dotenv(find_dotenv())

VIDEO_WRITE_PATH = os.environ.get("VIDEO_WRITE_PATH", "~/Videos")
BAR_END_RADIUS = 25.0


def _inference_to_bb(
    job: Job, frame: cv2.typing.MatLike, filename: str
) -> list[int] | None:
    ok, inference = get_inference(frame=frame, name=filename)
    if not ok:
        logging.error(f"[{str(job)}] Error getting inference!")
        return None
    logging.info(f"[{str(job)}] λ-function {inference}")
    return utils.detect_bar(inference)


def analyze(
    task: Task,
    cap: cv2.VideoCapture,
    total_frames: int,
    job: Job,
    queue: Queue,
    output_extension=VideoExtension.AVI.value,
    track_mode=TrackMode.BARSPEED.value,
) -> Path:
    filename = Path(job.key).stem
    output_path = Path(
        f"{VIDEO_WRITE_PATH}/analyzed_{job.job_id}_{filename}{output_extension}"
    )
    logging.info(f"[{str(job)}] Will Output to: {output_path}")

    out = cv2.VideoWriter(
        filename=output_path,
        fourcc=cv2.VideoWriter_fourcc("M", "J", "P", "G"),
        fps=30,
        frameSize=(
            int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        ),
    )
    ok, first_frame = cap.read()
    if not ok:
        raise Exception("Could not read first frame of video file.")

    bar_bb = _inference_to_bb(job=job, frame=first_frame, filename=f"{job.job_id}")

    bar_tracker = cv2.TrackerCSRT_create()
    track_data = TrackData()

    if bar_bb != None:
        bar_tracker_ok = bar_tracker.init(first_frame, bar_bb)

    last_progress = 0.0
    while True:

        ok, frame = cap.read()
        if not ok:
            raise Exception(f"Could not read frame {frame_number + 1} of video file.")

        frame_number = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
        timer = cv2.getTickCount()

        bar_tracker_ok, bar_bb = bar_tracker.update(frame)

        fps = cv2.getTickFrequency() / (cv2.getTickCount() - timer)

        if bar_tracker_ok:
            new_point = utils.get_centroid(bar_bb)
            p1, p2 = utils.get_bounding_points(bar_bb)
            bar_end_h = abs(p2[0] - p1[0])
            bar_end_w = abs(p2[1] - p2[1])

            average_edge_px = (bar_end_h + bar_end_w) / 2

            track_data.add_point_to_pairs(point=new_point)
            track_data.update_velocity(
                point=new_point, scale=BAR_END_RADIUS / average_edge_px, fps=fps
            )

            overlay = frame.copy()
            alpha = 0.6

            for point_pair in track_data.point_pairs[-50:]:
                cv2.line(
                    overlay,
                    point_pair["start"],
                    point_pair["end"],
                    utils.velocity_to_color(
                        point_pair["velocity_y"],
                        track_data.max_velocity_y,
                        track_mode=track_mode,
                    ),
                    3,
                    lineType=cv2.LINE_AA,
                )

            overlayed_frame = cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0)

            cv2.putText(
                overlayed_frame,
                f"Rep: {str(int(track_data.reps))}",
                (100, 50),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.75,
                (0, 0, 255),
                2,
            )

        out.write(overlayed_frame)
        progress = (round(frame_number / total_frames, 3)) * 100.0

        if progress % 5 < 0.0001 and progress != last_progress:
            task.update_state(
                task_id=str(job), state="PROGRESS", meta={"progress": progress}
            )
            message = {"job_id": job.job_id, "progress": f"{progress:.0f}"}
            queue.send_message(message=json.dumps(message))
            last_progress = progress

        if frame_number == total_frames:
            return output_path
