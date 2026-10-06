# ============================================================
# rail_stops_check.py — LOCAL diagnostic (does NOT touch AWS)
#
# Question it answers: what do DB RAIL trips in the live feed actually
# say about their stops? Which stop IDs and names do they use, and are
# the IDs even filled in?
#
# Run from D:\AWS\db with the venv active:
#     python rail_stops_check.py              (downloads the live feed now)
#     python rail_stops_check.py test.pb      (uses a saved feed file instead)
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

CITIES = [
    "Berlin", "München", "Hamburg", "Stuttgart", "Düsseldorf", "Hannover",
    "Dresden", "Mainz", "Kiel", "Erfurt", "Magdeburg", "Potsdam",
    "Saarbrücken", "Schwerin", "Wiesbaden", "Bremen",
]

# ------------------------------------------------------------
# 1. Static timetable (all columns read as plain text)
# ------------------------------------------------------------
routes = pd.read_csv(os.path.join(GTFS_FOLDER, "routes.txt"), dtype=str)
trips = pd.read_csv(os.path.join(GTFS_FOLDER, "trips.txt"), dtype=str)
agency = pd.read_csv(os.path.join(GTFS_FOLDER, "agency.txt"), dtype=str)
stops = pd.read_csv(os.path.join(GTFS_FOLDER, "stops.txt"), dtype=str)

db_agency_ids = set(
    agency.loc[agency["agency_name"].str.contains("DB ", case=False, na=False), "agency_id"]
)
rail_route_ids = set(
    routes.loc[routes["agency_id"].isin(db_agency_ids) & (routes["route_type"] == "2"), "route_id"]
)
rail_trip_ids = set(trips.loc[trips["route_id"].isin(rail_route_ids), "trip_id"])
stop_name = dict(zip(stops["stop_id"], stops["stop_name"]))

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

# ------------------------------------------------------------
# 3. Walk the feed, looking only at DB rail trips
# ------------------------------------------------------------
rail_trips_live = 0
rail_updates = 0
rail_empty_stop_id = 0
rail_unknown_stop_id = 0
name_counter = Counter()
raw_examples = []

all_updates = 0
all_empty_stop_id = 0

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue
    tu = entity.trip_update
    is_rail = tu.trip.trip_id in rail_trip_ids
    if is_rail:
        rail_trips_live += 1
    for stu in tu.stop_time_update:
        all_updates += 1
        if stu.stop_id == "":
            all_empty_stop_id += 1
        if not is_rail:
            continue
        rail_updates += 1
        if len(raw_examples) < 4:
            raw_examples.append(str(stu))
        if stu.stop_id == "":
            rail_empty_stop_id += 1
            continue
        name = stop_name.get(stu.stop_id)
        if name is None:
            rail_unknown_stop_id += 1
        else:
            name_counter[name] += 1

# ------------------------------------------------------------
# 4. Report
# ------------------------------------------------------------
print("\n=== DB RAIL TRIPS IN THE FEED RIGHT NOW ===")
print(f"Live DB rail trips (route_type 2):          {rail_trips_live}")
print(f"Stop updates inside those trips:            {rail_updates}")
if rail_trips_live:
    print(f"  average stop updates per trip:            {rail_updates / rail_trips_live:.1f}")
print(f"  with an EMPTY stop_id:                    {rail_empty_stop_id}")
print(f"  with a stop_id NOT found in stops.txt:    {rail_unknown_stop_id}")
print(f"(whole feed: {all_updates} stop updates, {all_empty_stop_id} with an empty stop_id)")

print("\n=== WHAT ONE RAW STOP UPDATE LOOKS LIKE (first 4 from DB rail trips) ===")
for i, text in enumerate(raw_examples, 1):
    print(f"--- example {i} ---")
    print(text.strip())

print("\n=== 25 STOP NAMES MOST OFTEN VISITED BY DB RAIL TRIPS ===")
for name, count in name_counter.most_common(25):
    print(f"  {count:5d}  {name}")

print("\n=== PER CITY: stop names used by DB rail trips that contain the city name ===")
for city in CITIES:
    matching = {n: c for n, c in name_counter.items() if city.lower() in n.lower()}
    total = sum(matching.values())
    print(f"\n{city}: {total} stop updates across {len(matching)} different stop names")
    for n, c in sorted(matching.items(), key=lambda kv: -kv[1])[:5]:
        print(f"    {c:4d}  {n}")