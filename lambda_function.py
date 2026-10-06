# ============================================================
# lambda_function.py  — Lambda "db-delay-ingestion"  (version 2)
#
# What changed compared with version 1:
#   * The static timetable (agency/routes/stops/trips) is no longer baked into the
#     deployment zip. It is read from S3 (gtfs_static/current.json -> folder),
#     where the "db-gtfs-refresh" Lambda keeps it up to date.
#   * Every run logs 4 CloudWatch metrics (see emit_metrics) so we can alarm on them.
#   * The two loops over the live feed are merged into one (same results).
#   * IDs are read as plain text (dtype=str), so the ".0" workaround is gone.
# What did NOT change: which stations, the DB + route_type 2 filter, CSV columns,
# and the S3 path/name of the output files.
# ============================================================

import csv
import io
import json
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

target_station_names = [
    "Berlin Hbf", "München Hbf", "Hamburg Hbf", "Stuttgart Hbf",
    "Düsseldorf Hbf", "Hannover Hbf", "Dresden Hbf", "Mainz Hbf",
    "Kiel Hbf", "Erfurt Hbf", "Magdeburg Hbf", "Potsdam Hbf",
    "Saarbrücken Hbf", "Schwerin Hbf", "Wiesbaden Hbf", "Bremen Hbf"
]
BUCKET_NAME = "db-delay-pipeline-sricharan-274535057566-eu-central-1-an"
STATIC_PREFIX = "gtfs_static"
FEED_URL = "https://realtime.gtfs.de/realtime-free.pb"
METRIC_NAMESPACE = "DBDelayPipeline"

s3_client = boto3.client("s3")

# Remembered between runs while this Lambda instance stays "warm", so we only
# re-read the big static files when the refresh Lambda has published a new folder.
_cache = {"folder": None, "stop_id_to_station": None, "db_trip_ids": None}


def read_static_csv(folder, name, usecols):
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{folder}/{name}")
    return pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str, usecols=usecols)


def load_static():
    """Returns (stop_id_to_station, db_trip_ids, static_age_hours)."""
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{STATIC_PREFIX}/current.json")
    current = json.loads(obj["Body"].read())
    folder = current["folder"]

    if _cache["folder"] != folder:
        print(f"Loading static timetable from s3://{BUCKET_NAME}/{folder}/")
        stops = read_static_csv(folder, "stops.txt", ["stop_id", "stop_name", "parent_station"])
        agency = read_static_csv(folder, "agency.txt", ["agency_id", "agency_name"])
        routes = read_static_csv(folder, "routes.txt", ["route_id", "agency_id", "route_type"])
        trips = read_static_csv(folder, "trips.txt", ["trip_id", "route_id"])

        # Station lookup: every stop whose name contains a target name, plus that
        # stop's parent station (realtime data often reports the parent ID).
        stop_id_to_station = {}
        for name in target_station_names:
            matches = stops[stops["stop_name"].str.contains(name, case=False, na=False, regex=False)]
            for _, row in matches.iterrows():
                stop_id_to_station[row["stop_id"]] = name
                parent = row["parent_station"]
                if isinstance(parent, str) and parent.strip() != "":
                    stop_id_to_station[parent] = name

        # DB rail trips: agency name contains "DB " and route_type == 2
        db_agency_ids = set(agency.loc[agency["agency_name"].str.contains("DB ", case=False, na=False), "agency_id"])
        db_route_ids = set(routes.loc[
            routes["agency_id"].isin(db_agency_ids) & (routes["route_type"] == "2"), "route_id"
        ])
        db_trip_ids = set(trips.loc[trips["route_id"].isin(db_route_ids), "trip_id"])

        _cache.update(folder=folder, stop_id_to_station=stop_id_to_station, db_trip_ids=db_trip_ids)

    refreshed_at = datetime.fromisoformat(current["refreshed_at"])
    age_hours = (datetime.now(timezone.utc) - refreshed_at).total_seconds() / 3600
    return _cache["stop_id_to_station"], _cache["db_trip_ids"], age_hours


def emit_metrics(**values):
    """Print one JSON line in CloudWatch 'Embedded Metric Format'. CloudWatch turns
    it into metrics automatically — no extra IAM permission needed."""
    units = {"StaticAgeHours": "None"}
    print(json.dumps({
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": METRIC_NAMESPACE,
                "Dimensions": [[]],
                "Metrics": [{"Name": k, "Unit": units.get(k, "Count")} for k in values],
            }],
        },
        **values,
    }))


def lambda_handler(event, context):
    stop_id_to_station, db_trip_ids, static_age_hours = load_static()

    # ---- Pull live feed ----
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(FEED_URL, timeout=30)
    feed.ParseFromString(response.content)

    # ---- Match (one pass over the feed) ----
    pulled_at_dt = datetime.now(timezone.utc)
    live_trips = 0
    db_trips_matched = 0
    output_rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        live_trips += 1
        trip = entity.trip_update
        if trip.trip.trip_id not in db_trip_ids:
            continue
        db_trips_matched += 1
        for stu in trip.stop_time_update:
            station = stop_id_to_station.get(stu.stop_id)
            if station is not None:
                output_rows.append({
                    "pulled_at": pulled_at_dt.isoformat(),
                    "trip_id": trip.trip.trip_id,
                    "stop_id": stu.stop_id,
                    "station_name": station,
                    "delay_sec": stu.arrival.delay
                })

    print(f"SUMMARY: live trips={live_trips}, DB rail trips matched={db_trips_matched}, "
          f"rows at target stations={len(output_rows)}, static data age={static_age_hours:.1f} h")
    emit_metrics(
        MatchedRows=len(output_rows),
        DBTripsMatched=db_trips_matched,
        LiveTrips=live_trips,
        StaticAgeHours=round(static_age_hours, 2),
    )

    # ---- Build CSV in memory ----
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["pulled_at", "trip_id", "stop_id", "station_name", "delay_sec"])
    writer.writeheader()
    writer.writerows(output_rows)

    # ---- Upload to S3 ----
    s3_key = (
        f"raw/year={pulled_at_dt.year}/month={pulled_at_dt.month:02d}/"
        f"day={pulled_at_dt.day:02d}/delays_{pulled_at_dt.strftime('%Y%m%d_%H%M%S')}.csv"
    )
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())

    result_message = f"Uploaded {len(output_rows)} records to s3://{BUCKET_NAME}/{s3_key}"
    print(result_message)
    return {"statusCode": 200, "body": result_message}