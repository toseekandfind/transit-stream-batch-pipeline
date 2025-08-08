### GTFS static data

Place GTFS static files here under `gtfs_static/` (unzipped):

- `agency.txt`
- `stops.txt`
- `routes.txt`
- `trips.txt`
- `stop_times.txt`
- `calendar.txt` (optional)

Spark reads `routes.txt` (short and long names) to enrich daily aggregates. You can add more joins later.

