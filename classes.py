from enum import Enum
import pika
import logging


class Queue(object):
    def __init__(
        self,
        parameters: pika.ConnectionParameters,
        exchange_name: str,
        queue_name: str,
        routing_key_name: str,
    ):
        self.parameters = parameters
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.routing_key_name = routing_key_name

        self.connection, self.channel = self._connect_to_channel()

    def _connect_to_channel(self):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        channel.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.routing_key_name,
        )
        return connection, channel

    def send_message(self, message: str, quiet: bool = True):
        # connection, channel = self.connect_to_channel()
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        if not quiet:
            logging.info(f"[->] Sent {message} to {self.exchange_name}")

    def close(self):
        self.connection.close()


class Job(object):
    def __init__(self, **kwargs):
        self.bucket: str = kwargs.get("bucket")
        self.key: str = kwargs.get("key")
        self.job_id: str = kwargs.get("job_id")
        self.status: str = kwargs.get("status", Status.RECEIVED.value)
        self.artifact: str = kwargs.get("artifact")

    def __str__(self):
        return f"{self.job_id}_{self.key}"

    def update(self, status: Status, artifact: str | None = None) -> dict:
        self.status = status.value
        self.artifact = artifact
        return {
            "job_id": self.job_id,
            "status": self.status,
            "artifact": self.artifact,
        }

    def to_dict(self):
        return {
            "bucket": self.bucket,
            "key": self.key,
            "job_id": self.job_id,
            "status": self.status,
            "artifact": self.artifact,
        }


class Status(Enum):
    RECEIVED = "received"
    SUCCESS = "success"
    ANALYZING = "analyzing"
    FAILURE = "failure"


class VideoExtension(Enum):
    AVI = ".avi"
    WEBM = ".webm"
    OGG = ".ogg"


# BGR color order
class Color(Enum):
    RED = (0, 0, 255)
    ORANGE = (0, 165, 255)
    YELLOW = (0, 255, 255)
    GREEN = (0, 255, 0)
    BLUE = (255, 0, 0)


class TrackMode(Enum):
    BARSPEED = 1
    UPDOWN = 2


class TrackData:
    def __init__(self):
        self.points = []
        self.delta_pixels_y = 0
        self.reps = 0
        self.frames_up = 0
        self.frames_down = 0
        self.y_vel_list = []
        self.point_pairs = []
        self.max_velocity_y = 0

    def add_point_to_pairs(self, point: tuple[int, int]):
        if len(self.points) >= 2:

            self.point_pairs.append(
                {"start": self.points[-2]["point"], "end": point, "velocity_y": 0.0}
            )
            self.delta_pixels_y = self.points[-2]["point"][1] - point[1]

            if self.delta_pixels_y >= 5:
                self.frames_up += 1
                self.frames_down = 0

            if self.delta_pixels_y <= -1:
                self.frames_down += 1
                if self.frames_up >= 25:
                    self.frames_up = 0
                    self.reps += 1

    def update_velocity(self, point: tuple[int, int], scale: float, fps: int):
        velocity_y = round(((self.delta_pixels_y * scale) / (1 / fps)) / 1000, 2)
        self.y_vel_list.append(velocity_y)
        self.max_velocity_y = max(self.y_vel_list)

        self.points.append({"point": point, "velocity_y": velocity_y})

        if len(self.point_pairs) >= 1:
            self.point_pairs[-1]["velocity_y"] = velocity_y
