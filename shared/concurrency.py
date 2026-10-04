"""Bounded provider fan-out with execution context and cooperative cancellation."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextvars import copy_context
from .execution_context import check_execution_budget


def bounded_results(action, values, max_workers):
    if max_workers < 1:
        raise ValueError("Worker count must be positive")
    iterator = iter(values)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pending = set()
        exhausted = False
        try:
            while pending or not exhausted:
                while len(pending) < max_workers and not exhausted:
                    check_execution_budget()
                    try:
                        value = next(iterator)
                    except StopIteration:
                        exhausted = True
                    else:
                        pending.add(executor.submit(copy_context().run, action, value))
                if pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        yield future.result()
        finally:
            for future in pending:
                future.cancel()
