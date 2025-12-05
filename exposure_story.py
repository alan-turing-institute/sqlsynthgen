
import story_types as stypes
from typing import List,cast, TypedDict, Callable, ParamSpec, TypeVar, Optional
import datetime as dt
import numpy as np
from sqlsynthgen.utils import generate_time_series
Body_temperature = 3025315
measurement_type_concept_id_temp = 32817 # EHR measurement
unit_concept_id_temp = 9289  # degree Celsius

P = ParamSpec("P")
R = TypeVar("R", bound=List[float])
Generator_Func = Callable[P, R]

def random_normal(len:int, loc:float, scale:float) -> List[float]:
    return [np.random.normal(loc, scale) for _ in range(len)]

def time_series(len:int, mean: float, std: float, epsilon_std: float, drift: float) -> List[float]:
    return np.round(generate_time_series(len, 'random_walk',
                                                {'mean': mean,
                                                'std': std,
                                                'epsilon_std': epsilon_std, 'drift': drift})).tolist()
class exposure(TypedDict):
    unit_concept_id: int
    measurement_type_concept_id: int
    rate: float
    generator_name: str
    parameters: Optional[dict[str, float]]

class treatment(TypedDict):
    drug_concept_id: int
    exposure_type_concept_id: int
    threshold: float
    consecutive_exposure_readings: int

generators:dict[str, Generator_Func[[int], List[float]]] = {
    "body_temperature": time_series
}    

def run_generator(name: str, *args, **kwargs) -> float:
    fn = generators[name]       # get the function
    return fn(*args, **kwargs)  # call it with arbitrary args

def random_event_times(avg_rate: float, visit_occurrence: stypes.SqlRow) -> list[dt.datetime]:
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

def generate_measurement_rows_for_dates(
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    rate: float,
    src_stats: stypes.SrcStats,
) -> List[stypes.SqlRow]:
    """Generate measurement rows for a visit occurrence at specific event datetimes."""
    rows = []
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])

    list_of_exposures:List[exposure] = [
        {
            "unit_concept_id": unit_concept_id_temp,
            "measurement_type_concept_id": measurement_type_concept_id_temp,
            "generator_name": "body_temperature",
            "rate": rate,
            "parameters": {"mean": 37.0, "std": 0.5, "epsilon_std": 0.1, "drift": 0.05}
        }
    ]

    list_of_treatments:List[treatment] = [
        {
            "drug_concept_id": unit_concept_id_temp,
            "exposure_type_concept_id": measurement_type_concept_id_temp,
            "threshold": 38.0
        }
    ]
    
    for exp in list_of_exposures:
        event_datetimes = random_event_times(exp["rate"], visit_occurrence)
        values = run_generator(generators[exp["generator_name"]], len(event_datetimes), **exp["parameters"])

        for event_datetime, v in zip(event_datetimes, values):
            r: stypes.SqlRow = {
                "measurement_concept_id": Body_temperature,
                "person_id": person_id,
                "visit_occurrence_id": visit_occurrence_id,
                "measurement_datetime": event_datetime,
                "measurement_date": event_datetime.date(),
                "measurement_type_concept_id": exp["measurement_type_concept_id"],
                "unit_concept_id": exp["unit_concept_id"],
                "value_as_number": v,
            }
            rows.append(r)

        
    return rows