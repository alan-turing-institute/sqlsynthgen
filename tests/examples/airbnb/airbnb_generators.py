import datetime
import random
from typing import Optional, Generator, Tuple

from mimesis import Generic


def user_dates_provider():
    generic = Generic()
    date_account_created: datetime.date = generic.datetime.date(start=2010, end=2015)

    booking_date: Optional[datetime.date] = None
    if generic.choice([True, False]):
        booking_date = generic.datetime.date(
            start=date_account_created.year + 1, end=2016
        )

    return date_account_created, booking_date


def user_age_provider(query_results):
    mu: float = query_results[0][0]
    sigma: float = query_results[0][1]

    return random.gauss(mu, sigma)


def session_story() -> Generator[Tuple[str, dict], dict, None]:
    user: dict = yield (
        "users",  # the table name
        {}  # 0 or more column values
    )
    sessions_per_user: int = random.randint(10, 20)

    for _ in range(sessions_per_user):
        if random.random() < 0.8:
            # most often, the session is from the user's sign-up device...
            yield (
                "sessions",
                {"device_type": user["first_device_type"]}
            )
        else:
            # ...but sometimes it is from any device type
            yield "sessions", {}
