from timeit import default_timer
from contextlib import contextmanager


__all__ = ['measure_time']


@contextmanager
def measure_time():
    start = default_timer()
    yield lambda: default_timer() - start
