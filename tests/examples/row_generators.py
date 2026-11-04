import datetime as dt

class UniquConstaintTests:
    UCT2_VALUES = [
        ("Super", "Brie", "Tinker"),
        ("Super", "Gouda", "Tailor"),  # This violates uniqueness of a
        ("Turbo", "Gruyere", "Soldier"),
        ("Mega", "Stilton", "Sailor"),
        ("Hyper", "Camembert", "Rich man"),
    ]

    def __init__(self):
        self.UCT2_COUNTER = 0

    def timespan_generator(
        self,
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

    def boolean_pair(self, generic):
        return tuple(generic.random.choice([True, False]) for _ in range(2))

    def unique_constraint_test2(self):
        """This generator is hand-crafted to yield particular value. It works as a
        regression test against an earlier bug.
        """
        self.UCT2_COUNTER = min(self.UCT2_COUNTER, len(self.UCT2_VALUES) - 1)
        return_value = self.UCT2_VALUES[self.UCT2_COUNTER]
        self.UCT2_COUNTER += 1
        return return_value
