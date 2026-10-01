import pandas as pd
import os

# set this to the folder where you unzipped the GTFS files
gtfs_folder = "latest"  # <-- change this to your actual folder path

routes = pd.read_csv(os.path.join(gtfs_folder, "routes.txt"))
trips = pd.read_csv(os.path.join(gtfs_folder, "trips.txt"))
stops = pd.read_csv(os.path.join(gtfs_folder, "stops.txt"))
agency = pd.read_csv(os.path.join(gtfs_folder, "agency.txt"))

db_agencies = agency[agency['agency_name'].str.contains('DB ', case=False, na=False)]
#print(db_agencies[['agency_id', 'agency_name']].to_string())

db_agency_ids = db_agencies['agency_id'].tolist()

db_rail_routes = routes[
    (routes['agency_id'].isin(db_agency_ids)) &
    (routes['route_type'] == 2)
]

db_route_ids = db_rail_routes['route_id'].tolist()

db_trips = trips[trips['route_id'].isin(db_route_ids)]
db_trip_ids = set(db_trips['trip_id'].astype(str))

print(f"Total DB train trips: {len(db_trip_ids)}")

#print(f"Total DB rail routes: {len(db_rail_routes)}")
#print(db_rail_routes[['route_id', 'route_short_name', 'route_long_name', 'agency_id']].head(20))

#print(agency.head(20))
#print(routes['route_type'].value_counts())
#print(routes.head())