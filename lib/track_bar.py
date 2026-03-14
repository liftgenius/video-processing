import cv2
import os, subprocess as sp, uuid, json
from lib import boto3_utils, lambda_function, rabbitmq_utils, ffmpeg_utils
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

BAR_END_RADIUS = 25.0
VIDEO_EXTENSION = ".avi"
# VIDEO_EXTENSION = ".webm"
# VIDEO_EXTENSION = ".ogg"


#BGR color order
COLORS = {
    "red": (0, 0, 255),
    "orange": (0, 165, 255),
    "yellow": (0, 255, 255),
    "green": (0, 255, 0),
    "blue": (255, 0, 0)
}


def get_centroid(bbox):
    centroid = (
        int((bbox[0] + bbox[2]/2)),
        int((bbox[1] + bbox[3]/2))
    )

    return centroid


def pred_box_to_bbox(pred_box):

    bbox = []
    bbox.append(int(pred_box[0]))
    bbox.append(int(pred_box[1]))

    bbox.append(int(pred_box[2]-pred_box[0]))
    bbox.append(int(pred_box[3] - pred_box[1]))

    return bbox


# def base64_encode_path(string_literal):
#     string_bytes = string_literal.encode("ascii")
#     base64_bytes = base64.b64encode(string_bytes)
#     base64_string = base64_bytes.decode("ascii")

#     return str(base64_string)


def detect_bar(inference):
        pred_box = inference["pred_boxes"][0]
        return pred_box_to_bbox(pred_box)


def get_bounding_points(bbox):
    p1 = (int(bbox[0]), int(bbox[1]))
    p2 = (int(bbox[0] + bbox[2]), int(bbox[1] + bbox[3]))

    return p1, p2


def velocity_to_color(velocity_val, velocity_max, mode):

    if mode == "bar_speed":
        if abs(velocity_val) <= 0.10 * velocity_max or abs(velocity_val) <= 0.2:
            return COLORS["red"]
        if abs(velocity_val) <= 0.25 * velocity_max:
            return COLORS["orange"]
        if abs(velocity_val) <= 0.5 * velocity_max:
            return COLORS["yellow"]
        if abs(velocity_val) > 0.5 * velocity_max:
            return COLORS["green"]

    if mode == "up_down":
        if velocity_val < 0.0:
            return COLORS["blue"]
        if velocity_val >= 0.0:
            return COLORS["red"]
        else:
            return COLORS["yellow"]

def delete_file(filepath):
    try:
        os.remove(filepath)
    except OSError as e:
        print("Error: %s - %s" % (e.filename, e.strerror))

def analyze(job_id, presigned_url, mode="bar_speed"):

    ID = str(uuid.uuid4())
    # output_path = os.environ.get('WRITE_PATH') + ID + VIDEO_EXTENSION
    output_path = "/home/john/Videos/liftgenius/" + ID + VIDEO_EXTENSION

    # output_bucket_name = os.environ.get('PROD_OUTPUT_BUCKET')
    output_bucket_name = "video-output"
    output_name = ID + VIDEO_EXTENSION
    video = cv2.VideoCapture(presigned_url)
    if not video.isOpened():
        print("could not open video")
        return -1

    
    h = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
    w = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
    # out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc('V','P','0','9'), 30, (w, h))
    out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc('M','J','P','G'), 30, (w, h))
    # out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc(*'MP4V'), 30, (w, h))
    # out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc(*'theo'), 30, (w, h))
    ok, frame = video.read()
    if not ok:
        print("cannot read video file")
        return 0

    total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    
    points = []
    delta_pixels_y = 0
    reps = 0
    frames_up = 0
    frames_down = 0
    y_vel_list = []
    point_pairs = []
    max_velocity_y = 0


    bar_tracker = cv2.TrackerCSRT_create()
    # bar_tracker = cv2.TrackerMIL.create()

    inference = lambda_function.get_inference(frame, ID)
    if inference == -1:
        return -1

    bar_bb = detect_bar(inference)

    if bar_bb != None:
        bar_tracker_ok = bar_tracker.init(frame, bar_bb)

   
    while True:
        ok, frame = video.read()
        if not ok:
            print(frame)
            return -1

        frame_number = int(video.get(cv2.CAP_PROP_POS_FRAMES))
        timer = cv2.getTickCount()

        bar_tracker_ok, bar_bb = bar_tracker.update(frame)

        fps = cv2.getTickFrequency() / (cv2.getTickCount() - timer)


        if bar_tracker_ok:
            bar_centroid = get_centroid(bar_bb)

            point = [bar_centroid[0], bar_centroid[1]]

            if (len(points) >= 2):

                point_pairs.append({"start": points[-2]["point"], "end": point, "velocity_y": 0.0})
                delta_pixels_y = points[-2]["point"][1] - point[1]

                if(delta_pixels_y >= 5):
                    frames_up += 1
                    frames_down = 0
                    
                if(delta_pixels_y <= -1):
                    frames_down += 1
                    if(frames_up >= 25):
                        frames_up = 0
                        reps += 1


            p1, p2 = get_bounding_points(bar_bb)
            bar_end_h = abs(p2[0] - p1[0])
            bar_end_w = abs(p2[1] - p2[1])

            average_edge_px = (bar_end_h + bar_end_w) /2

            px_to_mm = (BAR_END_RADIUS/average_edge_px)

            velocity_y = ((delta_pixels_y * px_to_mm) / (1/fps)) / 1000
            velocity_y_rounded = round(velocity_y, 2)
            y_vel_list.append(velocity_y_rounded)

            max_velocity_y = max(y_vel_list)

            points.append({"point": point, "velocity_y": velocity_y_rounded})

            if len(point_pairs) >= 1:
                point_pairs[-1]["velocity_y"] = velocity_y_rounded

            overlay = frame.copy()
            alpha = 0.6

            for point_pair in point_pairs[-50:]:
                cv2.line(overlay, point_pair["start"], point_pair["end"], velocity_to_color(point_pair["velocity_y"], max_velocity_y, mode), 3, lineType=cv2.LINE_AA)
            
        
            overlayed_frame = cv2.addWeighted(overlay, alpha, frame, 1 - alpha, 0)

            cv2.circle(overlayed_frame, bar_centroid, 2, velocity_to_color(velocity_y_rounded, max_velocity_y, "bar_speed"), 2)
            cv2.putText(overlayed_frame, "Rep " + str(int(reps)), (100,50), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0,0,255), 2)


        out.write(overlayed_frame)


        # if cv2.waitKey(1) & 0xFF == ord('q'):
        #     break

        progress = (round(frame_number/total_frames, 3)) * 100.0

        output_url = None

        
        print(f" 🕒 Analysis Progress: {progress:.1f}%", end="\r", flush=True)
        if int(progress) % 5 == 4:
            message = json.dumps({"job_id": job_id, "progress": f"{progress:.0f}"})
            rabbitmq_utils.send_message(
                exchange_name="processing_jobs_exchange",
                queue_name="processing_jobs_queue",
                routing_key_name="processing_jobs_queue", 
                 message=message
            )
        
        if frame_number == total_frames:
            video.release()
            # output_url = f"file://{output_path}"

            #convert to web compatible format with ffmpeg
            message = json.dumps({"job_id": job_id, "status": "processing"})
            rabbitmq_utils.send_message(
                exchange_name="processing_jobs_exchange",
                queue_name="processing_jobs_queue",
                routing_key_name="processing_jobs_queue", 
                message=message
            )
            converted_video_path = f"/home/john/Videos/liftgenius/{ID}.webm"
            converted_video_name = f"{ID}.webm"
            ffmpeg_cmd = f"ffmpeg -i {output_path} -c:v libsvtav1 -preset 4 -crf 30 -g 240 -pix_fmt yuv420p10le -svtav1-params tune=0:film-grain=8 -c:a -b:a -y {converted_video_path}"
            process = sp.Popen(ffmpeg_cmd, stdout=None, stderr=sp.PIPE, universal_newlines=True, shell=True)
            for line in process.stderr:
                frame_number = ffmpeg_utils.parse_ffmpeg_frame(line)
                ffmpeg_progress = ffmpeg_utils.ffmpeg_progress(frame_number, total_frames)
                if ffmpeg_progress > 0:
                    print(f" 🕒 Conversion Progress: {ffmpeg_progress:.1f}%", end="\r", flush=True)
                    if int(ffmpeg_progress) % 5 == 4:
                        message = json.dumps({"job_id": job_id, "progress": f"{ffmpeg_progress:.0f}"})
                        rabbitmq_utils.send_message(
                            exchange_name="processing_jobs_exchange",
                            queue_name="processing_jobs_queue",
                            routing_key_name="processing_jobs_queue", 
                            message=message
                        )

            with open(converted_video_path, "rb") as f:
                response = boto3_utils.upload_object(output_bucket_name, converted_video_name, f)
                output_url = boto3_utils.create_presigned_url(output_bucket_name, converted_video_name)
                # print(response)
                # exit()
            if output_url:
                delete_file(output_path)
                return output_url
                # return presigned_url

    







    