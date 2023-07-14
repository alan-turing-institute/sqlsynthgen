import random
import datetime
from typing import Optional, Generator, Tuple

def user_dates_provider(generic):
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

def sessions_story():
    """Generate users and their sessions."""
    device_types = ["Mac Desktop", "Windows Desktop", "iPhone"]

    # a new user will be sent back to us with our randomly chosen device type
    user: dict = yield (
        "users",  # table name
        {
            "first_device_type": random.choice(device_types)
        }  # see 1. below
    )

    # create between 10 and 19 sessions per user
    sessions_per_user: int = random.randint(10, 20)

    for _ in range(sessions_per_user):
        if random.random() < 0.8:
            # most often, the session is from the user's sign-up device...
            yield (
                "sessions",
                {
                    "user_id": user["id"],  # see 2. below
                    "device_type": user["first_device_type"],
                }
            )
        else:
            # ...but sometimes it is from any device type
            yield (
                "sessions",
                {
                    "user_id": user["id"],
                    "device_type": random.choice(device_types)},
            )
