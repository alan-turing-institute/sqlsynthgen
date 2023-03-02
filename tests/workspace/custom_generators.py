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


def boolean_from_src_stats_generator(generic, src_stats):
    num_false = int(next(x for x, y in src_stats if y is False))
    num_true = int(next(x for x, y in src_stats if y is True))
    return generic.weighted_boolean_provider.bool(num_true / num_false)
