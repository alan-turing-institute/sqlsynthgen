import random


def my_card_func(stats):
    """Choose a weighted card type."""
    total = sum([x["the_count"] for x in stats])
    return random.choices(
        population=tuple(x["card_type"] for x in stats),
        weights=tuple(x["the_count"] / total for x in stats),
    )[0]
