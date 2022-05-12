import multiprocessing
from itertools import chain, islice
from multiprocessing import Pool
from typing import List, Iterator, TypeVar, Generator, Tuple
from collections.abc import Callable
from timeit import default_timer
from contextlib import contextmanager


__all__ = ['chunked', 'measure_time']


T = TypeVar('T')


def chunked(seq: Iterator[T], chunksize: int) -> Generator[List[T], None, None]:
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:
        try:
            yield list(chain([next(it)], islice(it, chunksize-1)))
        except StopIteration:
            break


@contextmanager
def measure_time():
    start = default_timer()
    yield lambda: default_timer() - start
