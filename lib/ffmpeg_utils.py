import re

def parse_ffmpeg_frame(line):
    data = re.split("(frame=\s*)(\d{1,})", line)
    try:
        return data[2]
    except:
        return -1

def ffmpeg_progress(frame_number, total_frames):
    if int(frame_number) > 0 and int(total_frames) > 0:
        return(round(int(frame_number)/int(total_frames), 3)) * 100.0
    else:
        return -1