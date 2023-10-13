"""Row generators for the CC HIC OMOP schema."""
import datetime as dt
import random
from typing import Any, Optional, Union, cast

SqlValue = Union[float, int, str, bool, dt.datetime, dt.date, None]
SqlRow = dict[str, SqlValue]
SrcStatsResult = list[SqlRow]
SrcStats = dict[str, SrcStatsResult]


def sample_from_sql_group_by(
    group_by_result: SrcStatsResult,
    weights_column: str,
    value_columns: Optional[Union[str, list[str]]] = None,
    filter_dict: Optional[dict[str, Any]] = None,
) -> Union[SqlValue, SqlRow, tuple[SqlValue, ...]]:
    """Sample a row from the result of a SQL `GROUP BY` query.

    Arguments:
        group_by_result: Result of the query. A list of rows, with each row being a
            dictionary with names of columns as keys.
        weights_column: Name of the column which holds the weights based on which to
            sample. Typically the result of a `COUNT(*)`.
        value_columns: Name(s) of the column(s) to include in the result. Either a
            string, for a single column, an iterable of strings, or `None` for all
            columns.
        filter_dict: Dictionary of `{name_of_column: value_it_must_have}`, to restrict
            the sampling to a subset of group_by_result.

    Returns:
        * a single value, if `value_columns` is a single column name,
        * a tuple of values, in the same order as `value_columns`, if one is provided
        * a dictionary of {name_of_column: value} if `value_columns` is `None`
    """
    if filter_dict is not None:

        def filter_func(row: dict) -> bool:
            for key, value in filter_dict.items():
                if row[key] != value:
                    return False
            return True

        group_by_result = [row for row in group_by_result if filter_func(row)]
        if not group_by_result:
            raise ValueError("No group_by_result left after filter")

    weights = [cast(int, row[weights_column]) for row in group_by_result]
    weights = [w if w >= 0 else 1 for w in weights]
    random_choice = random.choices(group_by_result, weights)[0]
    if isinstance(value_columns, str):
        return random_choice[value_columns]
    if value_columns is not None:
        values = tuple(random_choice[col] for col in value_columns)
        return values
    return random_choice


def birth_datetime(
    generic: Any, src_stats: SrcStats
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[dt.datetime]]:
    """Generate values for the four birth datetime columns of the OMOP schema.

    Samples from the src_stats["count_alive_by_birth_year"] result.
    """
    year_of_birth = cast(
        int,
        sample_from_sql_group_by(
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


def gender(src_stats: SrcStats) -> GenderRows:
    """Generate values for the four gender columns of the OMOP schema.

    Samples from the src_stats["count_gender"] result.
    """
    return cast(
        GenderRows,
        sample_from_sql_group_by(
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


def ethnicity_race(src_stats: SrcStats) -> EthnicityRaceRows:
    """Generate values for the six race and gender columns of the OMOP schema.

    Samples from the src_stats["count_ethnicity_race"] result.
    """
    return cast(
        EthnicityRaceRows,
        sample_from_sql_group_by(
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


def make_null() -> None:
    """Return `None`."""
    return None
