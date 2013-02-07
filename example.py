from reader import run_generator
import sys
from collections import defaultdict
from datetime import time
import csv

# map(route-id, map(time, list(list(delay-at-stop))))
# i.e. map from route-ids to maps from times to lists of lists of delays at stops
all_delays = defaultdict(lambda: defaultdict(list))

# Go through all runs from stdin
for run in run_generator(sys.stdin):
    d = run.departure
    departure_time = time(d.hour, d.minute)
    delays = []
    # filter out end stops, their times are confusing
    for stop in run.stops[1:-1]:
        delay = (stop.actual_departure - stop.scheduled_time).total_seconds()
        delays.append(delay)
    all_delays[run.route_id][departure_time].append(delays)

wr = csv.writer(sys.stdout)
for route_id, delaylists_by_time in sorted(all_delays.iteritems()):
    print route_id
    for time, delaylists in sorted(delaylists_by_time.iteritems()):
        # first min/max inner lists, then min/max the results to get
        # min/max for given departure of given route in the dataset
        wr.writerow([route_id, str(time), min(min(l) for l in delaylists), max(max(l) for l in delaylists)])

