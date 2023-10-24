"""Row generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Optional, Union, cast

from mimesis import Generic

SqlValue = Union[float, int, str, bool, dt.datetime, dt.date, None]
SqlRow = dict[str, SqlValue]
SrcStatsResult = list[SqlRow]
SrcStats = dict[str, SrcStatsResult]


def birth_datetime(
    generic: Generic, src_stats: SrcStats
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[dt.datetime]]:
    """Generate values for the four birth datetime columns of the OMOP schema.

    Samples from the src_stats["count_alive_by_birth_year"] result.
    """
    year_of_birth = cast(
        int,
        generic.sql_group_by_provider.sample(
            src_stats["count_alive_by_birth_year"], "num", value_columns="year_of_birth"
        ),
    )
    if year_of_birth is None:
        return None, None, None, None
    year_of_birth = round(float(year_of_birth))
    while True:
        # Very occasionally we pick a datetime that isn't valid for the given
        # year (think leap years), in which case we just try again.
        try:
            datetime_of_birth = generic.datetime.datetime()
            datetime_of_birth = datetime_of_birth.replace(year=year_of_birth)
            break
        except ValueError as e:
            if "day is out of range for month" in str(e):
                pass
            else:
                raise e
    day_of_birth = datetime_of_birth.day
    month_of_birth = datetime_of_birth.month
    return year_of_birth, month_of_birth, day_of_birth, datetime_of_birth


GenderRows = tuple[Optional[int], Optional[str], Optional[int]]


def gender(generic: Generic, src_stats: SrcStats) -> GenderRows:
    """Generate values for the four gender columns of the OMOP schema.

    Samples from the src_stats["count_gender"] result.
    """
    return cast(
        GenderRows,
        generic.sql_group_by_provider.sample(
            src_stats["count_gender"],
            "num",
            value_columns=[
                "gender_concept_id",
                "gender_source_value",
                "gender_source_concept_id",
            ],
        ),
    )


EthnicityRaceRows = tuple[
    Optional[int],
    Optional[str],
    Optional[int],
    Optional[int],
    Optional[str],
    Optional[int],
]


def ethnicity_race(generic: Generic, src_stats: SrcStats) -> EthnicityRaceRows:
    """Generate values for the six race and gender columns of the OMOP schema.

    Samples from the src_stats["count_ethnicity_race"] result.
    """
    return cast(
        EthnicityRaceRows,
        generic.sql_group_by_provider.sample(
            src_stats["count_ethnicity_race"],
            "num",
            value_columns=[
                "race_concept_id",
                "race_source_value",
                "race_source_concept_id",
                "ethnicity_concept_id",
                "ethnicity_source_value",
                "ethnicity_source_concept_id",
            ],
        ),
    )
