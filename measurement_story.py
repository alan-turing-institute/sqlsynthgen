
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
class requirements(TypedDict):
    unit_concept_id: int
    measurement_type_concept_id: int
    rate: float
    generator_name: str

generators:dict[str, Generator_Func[[int], List[float]]] = {
    "body_temperature": random_normal
}    

def run_generator(name: str, *args, **kwargs) -> float:
    fn = generators[name]       # get the function
    return fn(*args, **kwargs)  # call it with arbitrary args

def random_normal(mean: float, std_dev: Optional[float] = None) -> float:
    """Return a normal distributed value with the given mean and standard deviation.

    If no standard devation is given, we assume it to be sqrt(abs(mean)).
    """
    return cast(
        float,
        np.random.normal(mean, std_dev if std_dev is not None else np.sqrt(abs(mean))),
    )

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

    list_of_requirements:List[requirements] = [
        {
            "unit_concept_id": unit_concept_id_temp,
            "measurement_type_concept_id": measurement_type_concept_id_temp,
            "generator_name": "body_temperature",
            "rate": rate
        }
    ]
    
    for req in list_of_requirements:
        event_datetimes = random_event_times(req["rate"], visit_occurrence)
        values = run_generator(generators[req["generator_name"]], len(event_datetimes), 37.0, 0.5)
        for event_datetime, v in zip(event_datetimes, values):
            r: stypes.SqlRow = {
                "measurement_concept_id": Body_temperature,
                "person_id": person_id,
                "visit_occurrence_id": visit_occurrence_id,
                "measurement_datetime": event_datetime,
                "measurement_date": event_datetime.date(),
                "measurement_type_concept_id": req["measurement_type_concept_id"],
                "unit_concept_id": req["unit_concept_id"],
                "value_as_number": v,
            }
            rows.append(r)
    return rows