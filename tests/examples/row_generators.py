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


def boolean_pair(generic):
    return tuple(generic.random.choice([True, False]) for _ in range(2))


# See the below function unique_constraint_test2 for the use of these.
UCT2_COUNTER = 0
UCT2_VALUES = [
    ("Super", "Brie", "Tinker"),
    ("Super", "Gouda", "Tailor"),  # This violates uniqueness of a
    ("Turbo", "Gruyere", "Soldier"),
    ("Mega", "Stilton", "Sailor"),
    ("Hyper", "Camembert", "Rich man"),
]


def unique_constraint_test2():
    """This generator is hand-crafted to yield particular value. It works as a
    regression test against an earlier bug.
    """
    global UCT2_COUNTER
    UCT2_COUNTER = min(UCT2_COUNTER, len(UCT2_VALUES) - 1)
    return_value = UCT2_VALUES[UCT2_COUNTER]
    UCT2_COUNTER += 1
    return return_value
