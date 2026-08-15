"""Run analyze() on the latest stored frame's preview — validates the algorithm
against real captured data (no Celery/Redis needed)."""
import os

import boto3
import cv2
import numpy as np
from pymongo import MongoClient

from detector import analyze

db = MongoClient(os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))[
    os.environ.get("MONGODB_DB", "observatory")
]
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
    region_name=os.environ.get("S3_REGION", "us-east-1"),
)
bucket = os.environ.get("S3_BUCKET", "observatory-dev")

doc = db.frames.find_one({"preview_key": {"$ne": None}}, sort=[("captured_at", -1)])
if not doc:
    print("NO FRAME WITH PREVIEW FOUND")
    raise SystemExit(1)

print("latest frame:", doc["_id"], "| preview_key:", doc["preview_key"])
data = s3.get_object(Bucket=bucket, Key=doc["preview_key"])["Body"].read()
bgr = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
print("preview shape:", rgb.shape)
print("RESULT:", analyze(rgb))
