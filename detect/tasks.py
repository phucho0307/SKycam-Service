"""Celery tasks: scan for un-scored frames, and detect one frame.

Reads the JPEG preview from S3, runs the heuristic, writes cloud_score/is_cloudy back
to the Mongo `frames` doc, and raises an `alarms` record when cloudy.
"""
import os
from datetime import datetime, timezone

import boto3
import cv2
import numpy as np
from bson import ObjectId
from pymongo import MongoClient

from celery_app import app
from detector import analyze

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.environ.get("MONGODB_DB", "observatory")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "observatory-dev")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
SCAN_LIMIT = int(os.environ.get("DETECT_SCAN_LIMIT", "50"))

_db = MongoClient(MONGODB_URI)[MONGODB_DB]
_s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
)


@app.task(name="tasks.scan_unscored")
def scan_unscored():
    """Enqueue detection for frames that don't have a cloud_score yet."""
    n = 0
    for doc in _db.frames.find({"cloud_score": {"$exists": False}}).limit(SCAN_LIMIT):
        detect_frame.delay(str(doc["_id"]))
        n += 1
    return {"enqueued": n}


@app.task(name="tasks.detect_frame")
def detect_frame(frame_id: str):
    doc = _db.frames.find_one({"_id": ObjectId(frame_id)})
    if not doc:
        return {"frame_id": frame_id, "skipped": "not found"}

    # Prefer the small JPEG preview (web-decodable); FITS needs astropy, skip if only that.
    key = doc.get("preview_key")
    if not key:
        _db.frames.update_one({"_id": doc["_id"]}, {"$set": {"cloud_score": None, "is_cloudy": None}})
        return {"frame_id": frame_id, "skipped": "no preview"}

    obj = _s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = obj["Body"].read()
    bgr = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
    if bgr is None:
        return {"frame_id": frame_id, "skipped": "decode failed"}
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

    result = analyze(rgb)
    _db.frames.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "cloud_score": result["cloud_score"],
            "is_cloudy": result["is_cloudy"],
            "star_count": result["star_count"],
        }},
    )
    if result["is_cloudy"]:
        _db.alarms.insert_one({
            "device_id": doc.get("device_id"),
            "kind": "cloud",
            "frame_id": doc["_id"],
            "score": result["cloud_score"],
            "created_at": datetime.now(timezone.utc),
            "acknowledged": False,
        })

    return {"frame_id": frame_id, **result}
