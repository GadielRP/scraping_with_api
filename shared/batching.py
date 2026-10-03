"""Bounded iteration without materializing the input collection."""
from itertools import islice


def chunks(values, size):
    if size < 1:
        raise ValueError("Batch size must be positive")
    iterator = iter(values)
    while batch := list(islice(iterator, size)):
        yield batch
