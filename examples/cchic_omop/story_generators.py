"""Story generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Callable, Generator, Optional, Union, cast

import numpy as np
import row_generators as rg


def random_normal(mean: float, std_dev: Optional[float] = None) -> float:
    """Return a normal distributed value with the given mean and standard deviation.

    If no standard devation is given, we assume it to be sqrt(abs(mean)).
    """
    return cast(
        float,
        np.random.normal(mean, std_dev if std_dev is not None else np.sqrt(abs(mean))),
    )


def gen_death(
    person: rg.SqlRow, src_stats: rg.SrcStats
) -> Optional[tuple[str, rg.SqlRow]]:
    """Generate a row for the death table."""
    alive = rg.sample_from_sql_group_by(
        src_stats["count_alive_by_birth_year"],
        weights_column="num",
        value_columns="alive",
        filter_dict={"year_of_birth": person["year_of_birth"]},
    )
    if alive:
        return None

    age_at_death_days = abs(
        random_normal(cast(float, src_stats["avg_age_at_death"][0]["avg_age_days"]))
    )
    death_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
        days=age_at_death_days
    )
    return "death", {
        "person_id": person["person_id"],
        "death_datetime": death_datetime,
        "death_date": death_datetime.date(),
    }


def gen_observation_period(
    person: rg.SqlRow, visit_occurrence: rg.SqlRow, src_stats: rg.SrcStats
) -> tuple[str, rg.SqlRow]:
    """Generate a row for the observation_period table."""
    period_type_concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_observation_period_types"],
            weights_column="num",
            value_columns="period_type_concept_id",
        ),
    )
    (
        diff_start_sign,
        diff_end_sign,
        avg_diff_start,
        avg_diff_end,
    ) = cast(
        tuple[str, str, float, float],
        rg.sample_from_sql_group_by(
            src_stats["observation_period_date_diffs"],
            weights_column="num",
            value_columns=[
                "diff_start_sign",
                "diff_end_sign",
                "avg_diff_start",
                "avg_diff_end",
            ],
        ),
    )

    vo_start = cast(dt.date, visit_occurrence["visit_start_date"])
    if diff_start_sign == "0":
        start_date = vo_start
    else:
        if avg_diff_start >= 0:
            diff = np.random.poisson(avg_diff_start)
        else:
            diff = -np.random.poisson(-avg_diff_start)
        start_date = vo_start + dt.timedelta(days=diff)

    vo_end = cast(dt.date, visit_occurrence["visit_end_date"])
    if diff_end_sign == "0":
        end_date = vo_end
    else:
        if avg_diff_end >= 0:
            diff = np.random.poisson(avg_diff_end)
        else:
            diff = -np.random.poisson(-avg_diff_end)
        end_date = vo_end + dt.timedelta(days=diff)

    return "observation_period", {
        "observation_period_start_date": start_date,
        "observation_period_end_date": end_date,
        "person_id": person["person_id"],
        "period_type_concept_id": period_type_concept_id,
    }


def gen_visit_occurrence(
    person: rg.SqlRow, death: Optional[rg.SqlRow], src_stats: rg.SrcStats
) -> tuple[str, rg.SqlRow]:
    """Generate a row for the visit_occurrence table."""
    age_days_at_visit_start = abs(
        random_normal(
            cast(float, src_stats["avg_age_at_visit_start"][0]["avg_age_days"])
        )
    )
    visit_start_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
        days=age_days_at_visit_start
    )
    visit_length_hours = abs(
        random_normal(
            cast(float, src_stats["avg_visit_length"][0]["avg_visit_length_hours"])
        )
    )
    visit_end_datetime = visit_start_datetime + dt.timedelta(hours=visit_length_hours)
    if death:
        visit_start_datetime = min(
            visit_start_datetime, cast(dt.datetime, death["death_datetime"])
        )
        visit_end_datetime = min(
            visit_end_datetime, cast(dt.datetime, death["death_datetime"])
        )
    return (
        "visit_occurrence",
        {
            "person_id": person["person_id"],
            "visit_start_datetime": visit_start_datetime,
            "visit_start_date": visit_start_datetime.date(),
            "visit_end_datetime": visit_end_datetime,
            "visit_end_date": visit_end_datetime.date(),
        },
    )


def random_event_times(
    avg_rate: float, visit_occurrence: rg.SqlRow
) -> list[dt.datetime]:
    """Return random times during a visit, occurring roughly at the given rate."""
    start = cast(dt.datetime, visit_occurrence["visit_start_datetime"])
    end = cast(dt.datetime, visit_occurrence["visit_end_datetime"])
    period = end - start
    events_per_hour = abs(random_normal(avg_rate))
    period_hours = period.seconds / 3600
    num_events = int(round(events_per_hour * period_hours))
    datetimes = [
        start + period * cast(float, fraction)
        for fraction in np.random.uniform(size=num_events)
    ]
    return datetimes


def gen_events(  # pylint: disable=too-many-arguments
    avg_rate: float,
    visit_occurrence: rg.SqlRow,
    person: rg.SqlRow,
    generator_function: Callable[
        [int, int, dt.datetime, rg.SrcStats], Optional[rg.SqlRow]
    ],
    table_name: str,
    src_stats: rg.SrcStats,
) -> list[tuple[str, rg.SqlRow]]:
    """Generate events for a visit occurrence, at a given rate with a given generator.

    This is a utility function for generating multiple rows for one of the "event"
    tables (measurements, observation, etc.).
    """
    event_datetimes = random_event_times(avg_rate, visit_occurrence)
    events: list[tuple[str, rg.SqlRow]] = []
    for event_datetime in sorted(event_datetimes):
        event = generator_function(
            cast(int, person["person_id"]),
            cast(int, visit_occurrence["visit_occurrence_id"]),
            event_datetime,
            src_stats,
        )
        if event is not None:
            events.append((table_name, event))
    return events


def assign_categoricals(
    row: rg.SqlRow,
    categoricals_result: rg.SrcStatsResult,
    categorical_columns: list[str],
    filter_dict: dict[str, rg.SqlValue],
) -> rg.SqlRow:
    """Add to a row categorical variables sampled from a query result.

    This is a utility function for sampling from a group by query and assigning the
    results to a row dictionary, used by the event generators.
    """
    result = cast(
        tuple[rg.SqlValue, ...],
        rg.sample_from_sql_group_by(
            categoricals_result,
            weights_column="num",
            value_columns=categorical_columns,
            filter_dict=filter_dict,
        ),
    )
    for column_name, value in zip(categorical_columns, result):
        row[column_name] = value
    return row


def gen_condition_occurrence(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the condition_occurrence table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_condition_occurrences"],
            weights_column="num",
            value_columns="condition_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "condition_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "condition_start_datetime": event_datetime,
        "condition_start_date": event_datetime.date(),
    }

    categorical_columns = [
        "condition_concept_id",
        "condition_type_concept_id",
        "condition_status_concept_id",
        "stop_reason",
        "provider_id",
        "condition_source_value",
        "condition_source_concept_id",
        "condition_status_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["condition_occurrence_categoricals"],
            categorical_columns,
            filter_dict={"condition_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for condition concept of id {concept_id}")
        return None

    try:
        duration_category, avg_duration_hours = cast(
            tuple[str, float],
            rg.sample_from_sql_group_by(
                src_stats["condition_occurrence_duration"],
                weights_column="num",
                value_columns=["duration_category", "avg_duration_hours"],
                filter_dict={"condition_concept_id": concept_id},
            ),
        )
    except ValueError:
        print(f"No duration data for condition concept of id {concept_id}")
        return None
    if duration_category == "NULL":
        row["condition_end_datetime"] = None
        row["condition_end_date"] = None
    else:
        if duration_category == "start":
            end_datetime = event_datetime
        else:
            duration_hours = abs(random_normal(avg_duration_hours))
            end_datetime = event_datetime + dt.timedelta(hours=duration_hours)
        row["condition_end_datetime"] = end_datetime
        row["condition_end_date"] = end_datetime.date()
    return row


def gen_measurement(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the measurement table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_measurements"],
            weights_column="num",
            value_columns="measurement_concept_id",
        ),
    )
    row: rg.SqlRow = {
        "measurement_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "measurement_datetime": event_datetime,
        "measurement_date": event_datetime.date(),
    }

    categorical_columns = [
        "measurement_type_concept_id",
        "operator_concept_id",
        "value_as_concept_id",
        "unit_concept_id",
        "value_as_number_sign",
        "range_low_sign",
        "range_high_sign",
        "provider_id",
        "visit_detail_id",
        "measurement_source_value",
        "measurement_source_concept_id",
        "unit_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["measurement_categoricals"],
            categorical_columns,
            filter_dict={"measurement_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for measurement of id {concept_id}")
        return None

    for key in ("value_as_number", "range_low", "range_high"):
        key_sign = key + "_sign"
        sign = cast(str, row[key_sign])
        if sign == "NULL":
            row[key] = None
        else:
            try:
                avg = next(
                    cast(float, row["avg_value"])
                    for row in src_stats["avg_measurement_value_as_number"]
                    if row["measurement_concept_id"] == concept_id
                )
            except StopIteration:
                print(f"No mean value for measurement of id {concept_id}")
                return None
            # To fix: Improve generating negative values. This method produces too few
            # negative values for variables that can be negative.
            value = random_normal(avg)
            if sign == ">=0":
                value = abs(value)
            row[key] = value
        del row[key_sign]
    return row


def gen_device_exposure(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the device_exposure table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_device_exposures"],
            weights_column="num",
            value_columns="device_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "device_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "device_exposure_start_datetime": event_datetime,
        "device_exposure_start_date": event_datetime.date(),
    }
    categorical_columns = [
        "end_datetime_category",
        "device_type_concept_id",
        "unique_device_id",
        "quantity",
        "provider_id",
        "visit_detail_id",
        "device_source_value",
        "device_source_concept_id",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["device_exposure_categoricals"],
            categorical_columns,
            filter_dict={"device_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for device concept of id {concept_id}")
        return None

    end_datetime_category = row["end_datetime_category"]
    if end_datetime_category == "NULL":
        end_datetime = None
    elif end_datetime_category == "= start_datetime":
        end_datetime = event_datetime
    else:
        print(
            "Unprepared to handle a case when device exposure end datetime is not "
            "NULL or the same as start."
        )
        end_datetime = None
    del row["end_datetime_category"]
    row["device_exposure_end_datetime"] = end_datetime
    row["device_exposure_end_date"] = (
        end_datetime.date() if end_datetime is not None else None
    )
    return row


def gen_observation(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the observation table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_observations"],
            weights_column="num",
            value_columns="observation_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "observation_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "observation_datetime": event_datetime,
        "observation_date": event_datetime.date(),
    }

    categorical_columns = [
        "observation_type_concept_id",
        "value_as_concept_id",
        "value_as_string",
        "value_as_number_sign",
        "qualifier_concept_id",
        "unit_concept_id",
        "provider_id",
        "visit_detail_id",
        "observation_source_value",
        "observation_source_concept_id",
        "unit_source_value",
        "qualifier_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["observation_categoricals"],
            categorical_columns,
            filter_dict={"observation_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for observation concept of id {concept_id}")
        return None

    key = "value_as_number"
    key_sign = key + "_sign"
    sign = row[key_sign]
    if sign == "NULL":
        row[key] = None
    else:
        try:
            avg = next(
                cast(float, row["avg_value"])
                for row in src_stats["avg_observation_value_as_number"]
                if row["observation_concept_id"] == concept_id
            )
        except StopIteration:
            print(f"No mean value for observation of id {concept_id}")
            return None
        # To fix: Improve generating negative values. This method produces too few
        # negative values for variables that can be negative.
        value = random_normal(avg)
        if sign == ">=0":
            value = abs(value)
        row[key] = value
    del row[key_sign]
    return row


def gen_procedure_occurrence(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the procedure_occurrence table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_procedure_occurrences"],
            weights_column="num",
            value_columns="procedure_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "procedure_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "procedure_datetime": event_datetime,
        "procedure_date": event_datetime.date(),
    }

    categorical_columns = [
        "procedure_type_concept_id",
        "modifier_concept_id",
        "quantity",
        "provider_id",
        "visit_detail_id",
        "procedure_source_value",
        "procedure_source_concept_id",
        "modifier_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["procedure_occurrence_categoricals"],
            categorical_columns,
            filter_dict={"procedure_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for procedure concept of id {concept_id}")
        return None
    return row


def gen_specimen(
    person_id: int,
    _: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the specimen table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_specimens"],
            weights_column="num",
            value_columns="specimen_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "specimen_concept_id": concept_id,
        "person_id": person_id,
        "specimen_datetime": event_datetime,
        "specimen_date": event_datetime.date(),
    }

    categorical_columns = [
        "specimen_type_concept_id",
        "quantity",
        "unit_concept_id",
        "anatomic_site_concept_id",
        "disease_status_concept_id",
        "specimen_source_id",
        "specimen_source_value",
        "unit_source_value",
        "anatomic_site_source_value",
        "disease_status_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["specimen_categoricals"],
            categorical_columns,
            filter_dict={"specimen_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for specimen concept of id {concept_id}")
        return None
    return row


def gen_drug_exposure(
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: rg.SrcStats,
) -> Optional[rg.SqlRow]:
    """Generate a row for the drug_exposure table."""
    concept_id = cast(
        int,
        rg.sample_from_sql_group_by(
            src_stats["count_drug_exposures"],
            weights_column="num",
            value_columns="drug_concept_id",
        ),
    )

    row: rg.SqlRow = {
        "drug_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "drug_exposure_start_datetime": event_datetime,
        "drug_exposure_start_date": event_datetime.date(),
    }

    categorical_columns = [
        "end_datetime_is_null",
        "verbatim_end_date",
        "drug_type_concept_id",
        "stop_reason",
        "refills",
        "days_supply",
        "sig",
        "route_concept_id",
        "lot_number",
        "provider_id",
        "drug_source_value",
        "drug_source_concept_id",
        "route_source_value",
        "dose_unit_source_value",
    ]
    try:
        assign_categoricals(
            row,
            src_stats["drug_exposure_categoricals"],
            categorical_columns,
            filter_dict={"drug_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for drug concept of id {concept_id}")
        return None

    if row["end_datetime_is_null"] == "NULL":
        row["drug_exposure_end_datetime"] = None
        row["drug_exposure_end_date"] = None
    else:
        try:
            duration_category, avg_duration_hours = cast(
                tuple[str, float],
                rg.sample_from_sql_group_by(
                    src_stats["drug_exposure_duration"],
                    weights_column="num",
                    value_columns=["duration_category", "avg_duration_hours"],
                    filter_dict={"drug_concept_id": concept_id},
                ),
            )
        except ValueError:
            print(f"No duration data for drug concept of id {concept_id}")
            return None
        if duration_category == "0":
            end_datetime = event_datetime
        else:
            duration_hours = abs(random_normal(avg_duration_hours))
            end_datetime = event_datetime + dt.timedelta(hours=duration_hours)
        row["drug_exposure_end_datetime"] = end_datetime
        row["drug_exposure_end_date"] = end_datetime.date()
    del row["end_datetime_is_null"]

    try:
        quantity_category, avg_quantity = cast(
            tuple[str, float],
            rg.sample_from_sql_group_by(
                src_stats["drug_exposure_quantity"],
                weights_column="num",
                value_columns=["quantity_category", "avg_quantity"],
                filter_dict={"drug_concept_id": concept_id},
            ),
        )
    except ValueError:
        print(f"No quantity data for drug concept of id {concept_id}")
        return None
    if quantity_category == "NULL":
        quantity: Union[int, float, None] = None
    elif quantity_category == "1":
        quantity = 1
    else:
        quantity = abs(random_normal(avg_quantity))
    row["quantity"] = quantity
    return row


def patient_story(
    src_stats: rg.SrcStats,
) -> Generator[tuple[str, rg.SqlRow], rg.SqlRow, None]:
    """Yield all the data related to a single patient.

    This includes, in order
    * a row for the `person` table
    * possibly a row for `death`, if the patient has died
    * rows for `visit_occurence` and `observation_period`
    * possibly multiple rows, depending on the length of the hospital stay, for
        * `condition_occurrence`
        * `measurement`
        * `device_exposure`
        * `observation`
        * `procedure_occurrence`
        * `specimen`
        * `drug_exposure`
    """
    person = yield "person", {}
    death = gen_death(person, src_stats)
    death_row = (yield death) if death else None
    visit_occurrence = yield gen_visit_occurrence(person, death_row, src_stats)
    yield gen_observation_period(person, visit_occurrence, src_stats)

    for event in gen_events(
        cast(float, src_stats["avg_condition_occurrences_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_condition_occurrence,
        "condition_occurrence",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_measurements_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_measurement,
        "measurement",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_device_exposures_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_device_exposure,
        "device_exposure",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_observations_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_observation,
        "observation",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_procedure_occurrences_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_procedure_occurrence,
        "procedure_occurrence",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_specimens_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_specimen,
        "specimen",
        src_stats,
    ):
        yield event

    for event in gen_events(
        cast(float, src_stats["avg_drug_exposures_per_hour"][0]["avg_per_hour"]),
        visit_occurrence,
        person,
        gen_drug_exposure,
        "drug_exposure",
        src_stats,
    ):
        yield event
