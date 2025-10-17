from typing import Optional
from enum import Enum
from mimesis import Generic
from mimesis.locales import Locale
generic = Generic(locale=Locale.EN_GB)
import random

def gender_provider(query_results) -> int:
    return generic.choice(
        items=[value for d in query_results for value in d.values()]
    )

def race_provider(query_results) -> int:
    return generic.choice(
        items=[value for d in query_results for value in d.values()]
    )

def ethnicity_provider(query_results) -> int:
    return generic.choice(
        items=[value for d in query_results for value in d.values()]
    )

def year_of_birth_provider(query_results) -> int:
   mean = query_results[0]['mean']
   std_dev = query_results[0]['std_dev']
   return int(random.gauss(mean, std_dev))