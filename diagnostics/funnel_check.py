# ============================================================
# funnel_check.py — LOCAL diagnostic (does NOT touch AWS)
#
# Question it answers: of everything in the live feed, where do
# trains "drop out" before they reach our 16 stations?
#
# Run from D:\AWS\db with the venv active:
#     python funnel_check.py              (downloads the live feed now)
#     python funnel_check.py test.pb      (uses a saved feed file instead)
#
# It only READS files in .\gtfs_data and the live feed. It changes nothing.
# ============================================================

import os
import sys
from collections import Counter

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

GTFS_FOLDER = "gtfs_data"
FEED_URL = "https://realtime.gtfs.de/realtime-free.pb"

target_station_names = [
    "Berlin Hbf", "München Hbf", "Hamburg Hbf", "Stuttgart Hbf",
    "Düsseldorf Hbf", "Hannover Hbf", "Dresden Hbf", "Mainz Hbf",
    "Kiel Hbf", "Erfurt Hbf", "Magdeburg Hbf", "Potsdam Hbf",
    "Saarbrücken Hbf", "Schwerin Hbf", "Wiesbaden Hbf", "Bremen Hbf",
]

# ------------------------------------------------------------
# 1. Load the static timetable. dtype=str keeps every ID as plain
#    text, so nothing gets turned into a number or a "123.0".
# ------------------------------------------------------------
routes = pd.read_csv(os.path.join(GTFS_FOLDER, "routes.txt"), dtype=str)
trips = pd.read_csv(os.path.join(GTFS_FOLDER, "trips.txt"), dtype=str)
agency = pd.read_csv(os.path.join(GTFS_FOLDER, "agency.txt"), dtype=str)
stops = pd.read_csv(os.path.join(GTFS_FOLDER, "stops.txt"), dtype=str)

# ------------------------------------------------------------
# 2. Our target stop_ids: every stop whose name contains one of the
#    16 station names, plus that stop's parent station (if any).
# ------------------------------------------------------------
stop_to_station = {}
for name in target_station_names:
    hits = stops[stops["stop_name"].str.contains(name, case=False, na=False, regex=False)]
    for _, row in hits.iterrows():
        stop_to_station[row["stop_id"]] = name
        parent = row["parent_station"]
        if isinstance(parent, str) and parent.strip() != "":
            stop_to_station[parent] = name

print("=== TARGET STOP IDS (from the static timetable) ===")
for name in target_station_names:
    count = sum(1 for s in stop_to_station.values() if s == name)
    print(f"  {name}: {count} ids")
print(f"  TOTAL: {len(stop_to_station)}")

# ------------------------------------------------------------
# 3. Which routes belong to DB, and what route_type values do they use?
# ------------------------------------------------------------
db_agency_ids = set(
    agency.loc[agency["agency_name"].str.contains("DB ", case=False, na=False), "agency_id"]
)
routes["is_db"] = routes["agency_id"].isin(db_agency_ids)

print("\n=== ROUTE TYPES USED BY DB AGENCIES (number of routes) ===")
print(routes.loc[routes["is_db"], "route_type"].value_counts().to_string())

# trip_id -> (route_id, route_type, route_short_name, is_db)
trip_info = (
    trips[["trip_id", "route_id"]]
    .drop_duplicates("trip_id")
    .merge(
        routes[["route_id", "route_type", "route_short_name", "is_db"]],
        on="route_id",
        how="left",
    )
    .set_index("trip_id")
)

# ------------------------------------------------------------
# 4. Get the live feed (from a saved file if one was given)
# ------------------------------------------------------------
feed = gtfs_realtime_pb2.FeedMessage()
if len(sys.argv) > 1:
    with open(sys.argv[1], "rb") as f:
        feed.ParseFromString(f.read())
    print(f"\n(using saved feed file: {sys.argv[1]})")
else:
    feed.ParseFromString(requests.get(FEED_URL, timeout=60).content)

# ------------------------------------------------------------
# 5. Walk every trip update once and record what we see
# ------------------------------------------------------------
rows = []
station_hits_any_trip = Counter()
seen_stop_ids = set()
total_stop_updates = 0

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue
    tu = entity.trip_update
    n_updates = 0
    hit_stations = []
    for stu in tu.stop_time_update:
        n_updates += 1
        seen_stop_ids.add(stu.stop_id)
        station = stop_to_station.get(stu.stop_id)
        if station:
            hit_stations.append(station)
            station_hits_any_trip[station] += 1
    total_stop_updates += n_updates
    rows.append({
        "trip_id": tu.trip.trip_id,
        "n_stop_updates": n_updates,
        "n_target_hits": len(hit_stations),
        "stations": "|".join(sorted(set(hit_stations))),
    })

live = pd.DataFrame(rows)
live = live.merge(trip_info, left_on="trip_id", right_index=True, how="left")
live["in_static"] = live["route_id"].notna()
live["is_db_flag"] = live["is_db"].fillna(False).astype(bool)

# ------------------------------------------------------------
# 6. Report
# ------------------------------------------------------------
print("\n=== FUNNEL (live feed right now) ===")
print(f"Entities in feed:                        {len(feed.entity)}")
print(f"Trip updates:                            {len(live)}")
print(f"  trip_id found in static timetable:     {int(live['in_static'].sum())}")
print(f"  ...of those, DB agency:                {int((live['in_static'] & live['is_db_flag']).sum())}")
print("  route_type of live DB trips:")
db_live = live[live["in_static"] & live["is_db_flag"]]
print(db_live["route_type"].value_counts().to_string())
print(f"Stop updates in feed (all trips):        {total_stop_updates}")
print(f"Stop updates AT A TARGET STATION (any trip, known or not): {sum(station_hits_any_trip.values())}")
print("  by station:", dict(station_hits_any_trip))

hit_trips = live[live["n_target_hits"] > 0]
print(f"\nTrips that stop at a target station:     {len(hit_trips)}")
print(f"  ...found in static timetable:          {int(hit_trips['in_static'].sum())}")
print(f"  ...DB agency:                          {int((hit_trips['in_static'] & hit_trips['is_db_flag']).sum())}")
print(f"  ...DB agency AND route_type 2 (what the Lambda keeps): "
      f"{int((hit_trips['in_static'] & hit_trips['is_db_flag'] & (hit_trips['route_type'] == '2')).sum())}")
print("\nSample of those trips:")
print(
    hit_trips[["trip_id", "in_static", "is_db_flag", "route_type", "route_short_name", "stations"]]
    .head(20)
    .to_string(index=False)
)

print("\n=== DOES THE FEED EVEN MENTION OUR STATION IDS? ===")
for name in target_station_names:
    ids = {sid for sid, st in stop_to_station.items() if st == name}
    present = len(ids & seen_stop_ids)
    print(f"  {name}: {present} of {len(ids)} ids appear anywhere in the feed")