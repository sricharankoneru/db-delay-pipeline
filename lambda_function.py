import pandas as pd
import os
import csv
import io
import boto3
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import requests

# Config — stays at module level so it's not rebuilt on every invocation
target_station_names = [
    "Berlin Hbf", "München Hbf", "Hamburg Hbf", "Stuttgart Hbf",
    "Düsseldorf Hbf", "Hannover Hbf", "Dresden Hbf", "Mainz Hbf",
    "Kiel Hbf", "Erfurt Hbf", "Magdeburg Hbf", "Potsdam Hbf",
    "Saarbrücken Hbf", "Schwerin Hbf", "Wiesbaden Hbf", "Bremen Hbf"
]
BUCKET_NAME = "db-delay-pipeline-sricharan-274535057566-eu-central-1-an"

# NOTE: the static GTFS files (routes.txt, trips.txt, agency.txt, stops.txt)
# need to be bundled INSIDE the Lambda deployment package under a folder,
# e.g. "gtfs_data/", since Lambda has no persistent local disk to download them to
# each time. We'll cover this in Stage 2.
GTFS_FOLDER = "gtfs_data"

def lambda_handler(event, context):
    # ---- Load static GTFS data ----
    routes = pd.read_csv(os.path.join(GTFS_FOLDER, "routes.txt"))
    trips = pd.read_csv(os.path.join(GTFS_FOLDER, "trips.txt"))
    agency = pd.read_csv(os.path.join(GTFS_FOLDER, "agency.txt"))
    stops = pd.read_csv(os.path.join(GTFS_FOLDER, "stops.txt"))

    # ---- Build station lookup ----
    stop_id_to_station = {}
    for name in target_station_names:
        matches = stops[stops['stop_name'].str.contains(name, case=False, na=False)]
        for stop_id in matches['stop_id'].astype(str):
            stop_id_to_station[stop_id] = name
    target_stop_ids = set(stop_id_to_station.keys())

    # ---- Filter to DB rail trip_ids ----
    db_agencies = agency[agency['agency_name'].str.contains('DB ', case=False, na=False)]
    db_agency_ids = db_agencies['agency_id'].tolist()
    db_rail_routes = routes[
        (routes['agency_id'].isin(db_agency_ids)) & (routes['route_type'] == 2)
    ]
    db_route_ids = db_rail_routes['route_id'].tolist()
    db_trips = trips[trips['route_id'].isin(db_route_ids)]
    db_trip_ids = set(db_trips['trip_id'].astype(str))

    # ---- Pull live feed ----
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get("https://realtime.gtfs.de/realtime-free.pb", timeout=30)
    feed.ParseFromString(response.content)

    # ---- DIAGNOSTIC LOGGING (new) ----
    print(f"DIAG: total entities in live feed: {len(feed.entity)}")

    db_match_count = 0
    station_match_count = 0
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.trip_id in db_trip_ids:
            db_match_count += 1
            for stu in entity.trip_update.stop_time_update:
                if stu.stop_id in target_stop_ids:
                    station_match_count += 1
    print(f"DIAG: DB trip matches (any station): {db_match_count}")
    print(f"DIAG: DB trip-stop pairs at target stations: {station_match_count}")
    # ---- END DIAGNOSTIC LOGGING ----

    # ---- Match and build CSV in memory ----
    pulled_at_dt = datetime.now(timezone.utc)
    output_rows = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip = entity.trip_update
            if trip.trip.trip_id in db_trip_ids:
                for stu in trip.stop_time_update:
                    if stu.stop_id in target_stop_ids:
                        output_rows.append({
                            "pulled_at": pulled_at_dt.isoformat(),
                            "trip_id": trip.trip.trip_id,
                            "stop_id": stu.stop_id,
                            "station_name": stop_id_to_station[stu.stop_id],
                            "delay_sec": stu.arrival.delay
                        })

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["pulled_at", "trip_id", "stop_id", "station_name", "delay_sec"])
    writer.writeheader()
    writer.writerows(output_rows)

    # ---- Upload to S3 ----
    s3_key = (
        f"raw/year={pulled_at_dt.year}/month={pulled_at_dt.month:02d}/"
        f"day={pulled_at_dt.day:02d}/delays_{pulled_at_dt.strftime('%Y%m%d_%H%M%S')}.csv"
    )
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())

    result_message = f"Uploaded {len(output_rows)} records to s3://{BUCKET_NAME}/{s3_key}"
    print(result_message)
    return {"statusCode": 200, "body": result_message}