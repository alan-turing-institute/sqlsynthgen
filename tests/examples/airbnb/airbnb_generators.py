import datetime
import random
from typing import Optional

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
