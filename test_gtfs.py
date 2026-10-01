# ============================================================
# DB Real-Time Delay Pipeline — Local Prototype (Multi-Station)
# ============================================================

import pandas as pd
import os
import csv
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import requests

# ------------------------------------------------------------
# STEP 0: Config — your 16 target stations
# Adjust names if any don't match well (we'll check the output)
# ------------------------------------------------------------
target_station_names = [
    "Berlin Hbf", "München Hbf", "Hamburg Hbf", "Stuttgart Hbf",
    "Düsseldorf Hbf", "Hannover Hbf", "Dresden Hbf", "Mainz Hbf",
    "Kiel Hbf", "Erfurt Hbf", "Magdeburg Hbf", "Potsdam Hbf",
    "Saarbrücken Hbf", "Schwerin Hbf", "Wiesbaden Hbf", "Bremen Hbf"
]

gtfs_folder = "latest"
output_folder = "output"
os.makedirs(output_folder, exist_ok=True)  # creates the folder if it doesn't exist

# ------------------------------------------------------------
# STEP 1: Load static GTFS files
# ------------------------------------------------------------
routes = pd.read_csv(os.path.join(gtfs_folder, "routes.txt"))
trips = pd.read_csv(os.path.join(gtfs_folder, "trips.txt"))
agency = pd.read_csv(os.path.join(gtfs_folder, "agency.txt"))
stops = pd.read_csv(os.path.join(gtfs_folder, "stops.txt"))

# ------------------------------------------------------------
# STEP 2: Build a stop_id -> station_name lookup for all 16 stations
# One station name can map to MULTIPLE stop_ids (see Berlin Hbf above)
# ------------------------------------------------------------
stop_id_to_station = {}  # e.g. {"120149": "Berlin Hbf", "394849": "Berlin Hbf", ...}

for name in target_station_names:
    matches = stops[stops['stop_name'].str.contains(name, case=False, na=False)]
    if matches.empty:
        print(f"WARNING: no match found for '{name}' — check spelling")
    for stop_id in matches['stop_id'].astype(str):
        stop_id_to_station[stop_id] = name

target_stop_ids = set(stop_id_to_station.keys())
print(f"Total stop_ids across all 16 stations: {len(target_stop_ids)}")

# Save this mapping to a file so you can review/reuse it later
with open(os.path.join(output_folder, "station_stop_id_mapping.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["stop_id", "station_name"])
    for stop_id, name in stop_id_to_station.items():
        writer.writerow([stop_id, name])

# ------------------------------------------------------------
# STEP 3: Filter to DB rail routes -> trip_ids
# ------------------------------------------------------------
db_agencies = agency[agency['agency_name'].str.contains('DB ', case=False, na=False)]
db_agency_ids = db_agencies['agency_id'].tolist()

db_rail_routes = routes[
    (routes['agency_id'].isin(db_agency_ids)) &
    (routes['route_type'] == 2)
]
db_route_ids = db_rail_routes['route_id'].tolist()

db_trips = trips[trips['route_id'].isin(db_route_ids)]
db_trip_ids = set(db_trips['trip_id'].astype(str))
print(f"Total DB train trips (static schedule): {len(db_trip_ids)}")

# ------------------------------------------------------------
# STEP 4: Download and parse live feed
# ------------------------------------------------------------
feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get("https://realtime.gtfs.de/realtime-free.pb", timeout=30)
feed.ParseFromString(response.content)
print(f"Total entities in live feed: {len(feed.entity)}")

# ------------------------------------------------------------
# STEP 5: Match DB trips AND target stations, upload to S3
# Instead of writing a local file, we build the CSV in memory
# and upload it directly to S3 under a date-partitioned path.
# ------------------------------------------------------------
import boto3
import io

pulled_at_dt = datetime.now(timezone.utc)
pulled_at = pulled_at_dt.isoformat()
output_rows = []

for entity in feed.entity:
    if entity.HasField("trip_update"):
        trip = entity.trip_update
        if trip.trip.trip_id in db_trip_ids:
            for stu in trip.stop_time_update:
                if stu.stop_id in target_stop_ids:
                    output_rows.append({
                        "pulled_at": pulled_at,
                        "trip_id": trip.trip.trip_id,
                        "stop_id": stu.stop_id,
                        "station_name": stop_id_to_station[stu.stop_id],
                        "delay_sec": stu.arrival.delay
                    })

print(f"Matched records at target stations: {len(output_rows)}")

# Build the CSV content in memory (a "virtual file") instead of writing to disk
csv_buffer = io.StringIO()
writer = csv.DictWriter(csv_buffer, fieldnames=["pulled_at", "trip_id", "stop_id", "station_name", "delay_sec"])
writer.writeheader()
writer.writerows(output_rows)

# ------------------------------------------------------------
# Build a date-partitioned S3 key, e.g.:
# raw/year=2026/month=09/day=11/delays_20260911_143000.csv
# This partitioning scheme is what Glue/Athena will use later
# to query efficiently by date instead of scanning everything.
# ------------------------------------------------------------
bucket_name = "db-delay-pipeline-sricharan-274535057566-eu-central-1-an"  # <-- CHANGE THIS to your actual bucket name

timestamp_for_filename = pulled_at_dt.strftime("%Y%m%d_%H%M%S")
s3_key = (
    f"raw/year={pulled_at_dt.year}/"
    f"month={pulled_at_dt.month:02d}/"
    f"day={pulled_at_dt.day:02d}/"
    f"delays_{timestamp_for_filename}.csv"
)

s3_client = boto3.client("s3")  # uses the credentials you set up via `aws configure`
s3_client.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_buffer.getvalue()
)

print(f"Uploaded to s3://{bucket_name}/{s3_key}")