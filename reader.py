from collections import defaultdict
from datetime import datetime, time

__all__ = ["Stop", "Run", "run_generator", "raw_run_generator", "sanity_filter"]

class Stop(object):
    """
    One stopping on a route. Contains number of the stop on the route,
    scheduled time, actual arrival time and actual departure time.
    """

    __slots__ = ["rank", "scheduled_time", "actual_arrival", "actual_departure"]
    def __init__(self, rank, scheduled_time, actual_arrival, actual_departure):
        self.rank = rank
        self.scheduled_time = scheduled_time
        self.actual_arrival = actual_arrival
        self.actual_departure = actual_departure

    def stop_length():
        """Return stop length in seconds."""
        return (self.actual_departure - self.actual_arrival).total_seconds()

class Run(object):
    """One run of a route.

    Run has run_id, which identifies the run uniquely. It is made of
    route_id which identifies the path of the run uniquely and
    departure timestamp. Route_id is in turn composed of line_id which
    identifies the the line and direction which tells whether the run
    was toward or away from the center."""

    __slots__ = ["run_id", "route_id", "line_code", "direction", "departure", "stops"]

    def __init__(self, run_id, route_id, line_code, direction, departure, stops):
        self.run_id = run_id
        self.route_id = route_id
        self.line_code = line_code
        self.direction = direction
        self.departure = departure
        self.stops = stops

def parse_hhmm(time_string):
    hours = int(time_string[:2])
    minutes = int(time_string[2:4])
    return time(hours, minutes)

def parse_hhmmss(time_string):
    ts = parse_hhmm(time_string)
    seconds = int(time_string[4:6])
    return time(hour=ts.hour, minute=ts.minute, second = seconds)


def parse_run(run_lines):
    """
    Parse a list of lines constituting a run into a Run object.
    """

    fields = run_lines[0].split(";")
    direction = int(fields[4])
    route_id = fields[2].split()[0] + " " + str(direction)
    date = datetime.strptime(fields[10], "%d/%m/%Y")


    route_code = fields[2].split()[0]

    stops = []
    for line in run_lines:
        fields = line.split(";")
        rank = int(fields[6])
        scheduled_time = datetime.combine(date, parse_hhmm(fields[9]))
        actual_arrival = datetime.combine(date, parse_hhmmss(fields[11]))
        actual_departure = datetime.combine(date, parse_hhmmss(fields[12]))
        stop_length = (actual_departure - actual_arrival)
        stops.append(Stop(rank, scheduled_time, actual_arrival, actual_departure))

    run_id = stops[0].scheduled_time.strftime(route_id + ":" + "%Y-%m-%dT%H:%M:%S")

    return Run(run_id, route_id, route_code, direction, stops[0].scheduled_time, stops)

def run_generator(source):
    """
    Generate sanity-filtered Runs from a source of lines. Consumes all
    input before generating any output, so may not be usable on large
    datasets.
    """
    return sanity_filter(raw_run_generator(source))

def raw_run_generator(source):
    """
    Generate Runs from a source of lines.
    """
    buffer = []
    for line in source:
        fields = line.split(";")
        
        rank = fields[6]
        if int(rank) < 0:
            continue
        if rank == "0":
            if buffer:
                yield parse_run(buffer)
            buffer = []
            buffer.append(line)
        else:
            buffer.append(line)
    yield parse_run(buffer)

def _singleton_run_filter(run_source):
    """
    Filter out singleton (one-stop) "runs".
    """
    for run in run_source:
        if len(run.stops) > 1:
            yield run

def _modal_length_run_filter(run_source):
    """
    Filter out non-singleton, modal-length runs.

    Consumes all input before producing any output.
    """

    all_runs = defaultdict(lambda: defaultdict(list))
    for run in run_source:
        route_id = run.route_id
        length = len(run.stops)
        if length:
            all_runs[route_id][length].append(run)

    for route_id, runs_by_length in sorted(all_runs.items()):
        modal_count, modal_length = list(sorted((len(v), k) for k,v in runs_by_length.items()))[-1]
        for run in runs_by_length[modal_length]:
            yield run


def sanity_filter(run_source):
    """
    Filter out singleton (one-stop) Runs and Runs that are not of the
    modal length for that route. Consumes all input before generating
    any output.
    """
    return _modal_length_run_filter(_singleton_run_filter(run_source))


