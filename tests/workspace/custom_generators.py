import datetime as dt


def timespan_generator(
    generic,
    earliest_start_year,
    last_start_year,
    min_dt_days,
    max_dt_days,
):
    min_dt = dt.timedelta(days=min_dt_days)
    max_dt = dt.timedelta(days=max_dt_days)
    start, end, delta = generic.timespan_provider.timespan(
        earliest_start_year, last_start_year, min_dt, max_dt
    )
    return start, end, delta.total_seconds()
