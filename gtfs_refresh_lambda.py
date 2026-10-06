# ============================================================
# gtfs_refresh_lambda.py  — Lambda "db-gtfs-refresh"
#
# Job: download the current gtfs.de static timetable, keep only the 4 small
# files we need (agency, routes, stops, trips) and save them to S3.
#
# S3 layout it creates:
#   gtfs_static/<YYYYmmdd_HHMMSS>/agency.txt, routes.txt, stops.txt, trips.txt
#   gtfs_static/current.json     <- points to the newest complete folder
#   gtfs_static/history/<YYYYmmdd_HHMMSS>.json  <- one log entry per refresh
#
# Why a new folder every time + current.json?
#   The ingestion Lambda reads current.json first, then the 4 files. current.json
#   is only updated AFTER all 4 files are saved, so the ingestion Lambda can never
#   read a half-updated (mismatched) set of files.
#
# Why the history entries?
#   Each one records the download's Last-Modified/ETag and a fingerprint (MD5)
#   of each file. After a few days we can SEE how often gtfs.de really changes
#   the timetable, instead of guessing.
# ============================================================

import hashlib
import json
import zipfile
from datetime import datetime, timezone

import shutil
import urllib.request

import boto3

BUCKET_NAME = "db-delay-pipeline-sricharan-274535057566-eu-central-1-an"
STATIC_URL = "https://download.gtfs.de/germany/free/latest.zip"
WANTED_FILES = ["agency.txt", "routes.txt", "stops.txt", "trips.txt"]
PREFIX = "gtfs_static"
ZIP_PATH = "/tmp/gtfs_latest.zip"  # Lambda's only writable folder


def download_zip(url, path):
    """Stream the big zip to disk (never hold 284 MB in memory). Returns headers.
    Uses Python's built-in urllib, so this Lambda needs NO extra packages."""
    req = urllib.request.Request(url, headers={"User-Agent": "db-delay-pipeline (learning project)"})
    with urllib.request.urlopen(req, timeout=120) as r, open(path, "wb") as f:
        shutil.copyfileobj(r, f, length=1024 * 1024)
        return {
            "last_modified": r.headers.get("Last-Modified"),
            "etag": r.headers.get("ETag"),
            "content_length": r.headers.get("Content-Length"),
        }


def copy_wanted_files(zip_path, s3_client, bucket, folder):
    """Copy only the wanted files out of the zip straight into S3. Returns {name: {md5, bytes}}."""
    info = {}
    with zipfile.ZipFile(zip_path) as zf:
        names = {n.split("/")[-1]: n for n in zf.namelist()}
        missing = [w for w in WANTED_FILES if w not in names]
        if missing:
            raise RuntimeError(f"Files missing from the downloaded zip: {missing}")
        for wanted in WANTED_FILES:
            md5 = hashlib.md5()
            with zf.open(names[wanted]) as src:
                data = src.read()  # these 4 files are small (about 75 MB total)
            md5.update(data)
            size = len(data)
            s3_client.put_object(Bucket=bucket, Key=f"{folder}/{wanted}", Body=data)
            info[wanted] = {"md5": md5.hexdigest(), "bytes": size}
    return info


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    folder = f"{PREFIX}/{stamp}"

    headers = download_zip(STATIC_URL, ZIP_PATH)
    print(f"Downloaded zip: {headers}")

    files = copy_wanted_files(ZIP_PATH, s3, BUCKET_NAME, folder)
    print(f"Saved {len(files)} files under s3://{BUCKET_NAME}/{folder}/")

    record = {"refreshed_at": now.isoformat(), "folder": folder, "source": headers, "files": files}

    # History first, "current" pointer LAST (so it only moves once everything is saved)
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{PREFIX}/history/{stamp}.json",
                  Body=json.dumps(record, indent=2))
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{PREFIX}/current.json",
                  Body=json.dumps({"folder": folder, "refreshed_at": now.isoformat()}))

    print(f"current.json now points to {folder}")
    return {"statusCode": 200, "body": json.dumps(record)}