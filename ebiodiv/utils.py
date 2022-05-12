import multiprocessing
from itertools import chain, islice
from multiprocessing import Pool
from typing import List, Iterator, TypeVar, Generator, Tuple
from collections.abc import Callable
from timeit import default_timer
from contextlib import contextmanager


__all__ = ['chunked', 'pool_map', 'get_worker_count']


def get_worker_count():
    return multiprocessing.cpu_count()


T = TypeVar('T')


def chunked(seq: Iterator[T], chunksize: int) -> Generator[List[T], None, None]:
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:
        try:
            yield list(chain([next(it)], islice(it, chunksize-1)))
        except StopIteration:
            break


# def _worker_wrapper(t: Tuple[Callable[..., List[T]], List[T]]) -> List[T]:
def _worker_wrapper(t):
    f, chunk = t
    g = globals()
    return f(*g['__args'], chunk, **g['__kwargs'])


def _init_worker(args, kwargs):
    globals()['__args'] = args
    globals()['__kwargs'] = kwargs


# def pool_map(f: Callable[..., List[T]], chunk_list: Iterator[List[T]], *args, **kwargs) -> List[T]:
def pool_map(f, chunk_list, *args, **kwargs):
    """Equivalent of multiprocessing.Pool.map() but allows additional arguments (*args and **kwargs).
    The additional parameters are serialized once per worker.

    Equivalent of the following code, but each call to f happens in a different processes to overcome the GIL:

    >>> result = []
    >>> for chunk in chunk_list:
    >>>     result.extend(f(*args, chunk, **kwargs))
    >>> return result

    Also equivalent of (but this pool_map is faster when *args and **kwargs takes a lot of memory):

    >>> import functools
    >>> result = []
    >>> with Pool(get_worker_count()) as p:
    >>>     fp = functool.partial(f, *args, **kwargs)
    >>>     for chunk in p.map(fp, chunk_list):
    >>>         result.extend(chunk)
    >>> return result

    """
    results = []

    worker_data: List[Tuple[Callable[..., List[T]], List[T]]] = [(f, chunk) for chunk in chunk_list]

    with Pool(
        processes=min(get_worker_count(), len(worker_data)),
        initializer=_init_worker,
        initargs=(args, kwargs),
    ) as p:
        for chunk in p.map(_worker_wrapper, worker_data):
            results.extend(chunk)

    return results


@contextmanager
def measure_time():
    start = default_timer()
    yield lambda: default_timer() - start
