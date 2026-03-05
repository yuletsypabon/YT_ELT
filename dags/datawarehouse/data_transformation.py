from datetime import timedelta, datetime


def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    return timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )


def transform_data(row):
    # row comes from RealDictCursor (staging SELECT) — all keys are lowercase db column names:
    # video_id, video_title, upload_date, duration, video_views, likes_count, comment_count

    # Convert RealDictRow to a plain mutable dict
    row = dict(row)

    duration_td = parse_duration(row["duration"])

    # Overwrite duration string with formatted time value
    row["duration"] = (datetime.min + duration_td).time()

    # Add video_type — new column only in core
    row["video_type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row