import os
from pathlib import Path
from classes import Color, TrackMode


def get_centroid(bbox: list[int]) -> tuple[int, int]:
    return tuple([int((bbox[0] + bbox[2] / 2)), int((bbox[1] + bbox[3] / 2))])


def pred_box_to_bbox(pred_box: list[int]) -> list[int]:

    bbox = []
    bbox.append(int(pred_box[0]))
    bbox.append(int(pred_box[1]))

    bbox.append(int(pred_box[2] - pred_box[0]))
    bbox.append(int(pred_box[3] - pred_box[1]))

    return bbox


# def base64_encode_path(string_literal):
#     string_bytes = string_literal.encode("ascii")
#     base64_bytes = base64.b64encode(string_bytes)
#     base64_string = base64_bytes.decode("ascii")

#     return str(base64_string)


def detect_bar(inference: dict) -> list[int]:
    pred_box: list[int] = inference["pred_boxes"][0]
    return pred_box_to_bbox(pred_box)


def get_bounding_points(bbox: list[int]) -> tuple[tuple[int, int], tuple[int, int]]:
    p1 = (int(bbox[0]), int(bbox[1]))
    p2 = (int(bbox[0] + bbox[2]), int(bbox[1] + bbox[3]))

    return (p1, p2)


def velocity_to_color(velocity_val: float, velocity_max: float, track_mode: TrackMode):

    match track_mode:
        case TrackMode.BARSPEED.value:
            if abs(velocity_val) <= 0.10 * velocity_max or abs(velocity_val) <= 0.2:
                return Color.RED.value
            if abs(velocity_val) <= 0.25 * velocity_max:
                return Color.ORANGE.value
            if abs(velocity_val) <= 0.5 * velocity_max:
                return Color.YELLOW.value
            if abs(velocity_val) > 0.5 * velocity_max:
                return Color.GREEN.value

        case TrackMode.UPDOWN.value:
            if velocity_val < 0.0:
                return Color.BLUE.value
            if velocity_val >= 0.0:
                return Color.RED.value
            else:
                return Color.YELLOW.value


def delete_file(filepath: Path):
    try:
        os.remove(filepath)
    except OSError as e:
        print("Error: %s - %s" % (e.filename, e.strerror))
