# ============================================================
# rail_mix_check.py — LOCAL diagnostic (does NOT touch AWS)
#
# Question it answers: how many REAL trains (ICE, IC, RE, RB, S-Bahn ...)
# does the live feed contain right now, and who operates them?
#
# Run from D:\AWS\db with the venv active:
#     python rail_mix_check.py              (downloads the live feed now)
#     python rail_mix_check.py test.pb      (uses a saved feed file instead)
#
# It only READS files in .\gtfs_data and the live feed. It changes nothing.
# ============================================================

import os
import sys

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

GTFS_FOLDER = "gtfs_data"
FEED_URL = "https://realtime.gtfs.de/realtime-free.pb"

# ------------------------------------------------------------
# 1. Static timetable (all columns read as plain text)
# ------------------------------------------------------------
routes = pd.read_csv(os.path.join(GTFS_FOLDER, "routes.txt"), dtype=str)
trips = pd.read_csv(os.path.join(GTFS_FOLDER, "trips.txt"), dtype=str)
agency = pd.read_csv(os.path.join(GTFS_FOLDER, "agency.txt"), dtype=str)
stops = pd.read_csv(os.path.join(GTFS_FOLDER, "stops.txt"), dtype=str)
stop_name = dict(zip(stops["stop_id"], stops["stop_name"]))

route_cols = ["route_id", "agency_id", "route_type", "route_short_name"]
if "route_long_name" in routes.columns:
    route_cols.append("route_long_name")

trip_info = (
    trips[["trip_id", "route_id"]]
    .drop_duplicates("trip_id")
    .merge(routes[route_cols], on="route_id", how="left")
    .merge(agency[["agency_id", "agency_name"]], on="agency_id", how="left")
    .set_index("trip_id")
)

# ------------------------------------------------------------
# 2. Live feed
# ------------------------------------------------------------
feed = gtfs_realtime_pb2.FeedMessage()
if len(sys.argv) > 1:
    with open(sys.argv[1], "rb") as f:
        feed.ParseFromString(f.read())
    print(f"(using saved feed file: {sys.argv[1]})")
else:
    feed.ParseFromString(requests.get(FEED_URL, timeout=60).content)

rows = []
for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue
    tu = entity.trip_update
    first_stops = [stop_name.get(s.stop_id, "?") for s in list(tu.stop_time_update)[:2]]
    rows.append({
        "trip_id": tu.trip.trip_id,
        "n_stop_updates": len(tu.stop_time_update),
        "first_stops": " -> ".join(first_stops),
    })

live = pd.DataFrame(rows).merge(trip_info, left_on="trip_id", right_index=True, how="left")
live["in_static"] = live["route_id"].notna()
known = live[live["in_static"]].copy()

# route_type 2 = rail in this timetable. Classify by the letters at the
# start of the line name: ICE, IC, RE, RB, S ...
rail = known[known["route_type"] == "2"].copy()
rail["prefix"] = (
    rail["route_short_name"].fillna("").str.upper().str.extract(r"^([A-Z]+)")[0].fillna("(none)")
)

print("\n=== LIVE TRIPS BY ROUTE TYPE (all operators) ===")
print(f"Trip updates in feed: {len(live)}   found in timetable: {len(known)}")
print(known["route_type"].value_counts().to_string())

print("\n=== ROUTE_TYPE 2 (rail) TRIPS: WHO OPERATES THEM? (top 12) ===")
print(rail["agency_name"].value_counts().head(12).to_string())

print("\n=== ROUTE_TYPE 2 TRIPS BY LINE-NAME LETTERS (top 20) ===")
print(rail["prefix"].value_counts().head(20).to_string())

print("\n=== SAME, BUT ONLY DB AGENCIES ===")
db_rail = rail[rail["agency_name"].str.contains("DB ", case=False, na=False)]
print(f"DB rail trips: {len(db_rail)}")
print(db_rail["prefix"].value_counts().head(15).to_string())

print("\n=== 15 RANDOM DB route_type 2 TRIPS: what are they really? ===")
cols = ["agency_name", "route_short_name", "n_stop_updates", "first_stops"]
if "route_long_name" in db_rail.columns:
    cols.insert(2, "route_long_name")
sample = db_rail.sample(n=min(15, len(db_rail)), random_state=1)
print(sample[cols].to_string(index=False))