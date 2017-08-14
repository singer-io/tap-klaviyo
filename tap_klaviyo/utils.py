import datetime
import time

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_ts(dt):
    return int(time.mktime(datetime.datetime.strptime(
        dt, DATETIME_FMT).timetuple()))


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATETIME_FMT)


def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, int):
        dt = ts_to_dt(dt)

    if entity not in state:
        state[entity] = dt

    if dt >= state[entity]:
        state[entity] = dt
