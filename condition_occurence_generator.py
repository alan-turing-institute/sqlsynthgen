def gen_condition_occurrence(
    generic: Generic,
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: SrcStats,
) -> Optional[SqlRow]:
    """Generate a row for the condition_occurrence table."""
    concept_id = cast(
        int,
        generic.sql_group_by_provider.sample(
            src_stats["count_condition_occurrences"],
            weights_column="num",
            value_columns="condition_concept_id",
        ),
    )

    row: SqlRow = {
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
            generic,
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
            generic.sql_group_by_provider.sample(
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

def patient_story(
    generic: Generic,
    src_stats: SrcStats,
) -> Generator[tuple[str, SqlRow], SqlRow, None]:
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
    death = gen_death(generic, person, src_stats)
    death_row = (yield death) if death else None
    visit_occurrence = yield gen_visit_occurrence(person, death_row, src_stats)
    yield gen_observation_period(generic, person, visit_occurrence, src_stats)

    def gen_events_for_patient(
        rate_query_name: str,
        gen_func: Callable[
            [Generic, int, int, dt.datetime, SrcStats], Optional[SqlRow]
        ],
        table_name: str,
    ) -> list[tuple[str, SqlRow]]:
        return gen_events(
            generic,
            cast(float, src_stats[rate_query_name][0]["avg_per_hour"]),
            visit_occurrence,
            person,
            gen_func,
            table_name,
            src_stats,
        )

    for event in gen_events_for_patient(
        "avg_condition_occurrences_per_hour",
        gen_condition_occurrence,
        "condition_occurrence",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_measurements_per_hour",
        gen_measurement,
        "measurement",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_device_exposures_per_hour",
        gen_device_exposure,
        "device_exposure",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_observations_per_hour",
        gen_observation,
        "observation",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_procedure_occurrences_per_hour",
        gen_procedure_occurrence,
        "procedure_occurrence",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_specimens_per_hour",
        gen_specimen,
        "specimen",
    ):
        yield event

    for event in gen_events_for_patient(
        "avg_drug_exposures_per_hour",
        gen_drug_exposure,
        "drug_exposure",
    ):
        yield event
