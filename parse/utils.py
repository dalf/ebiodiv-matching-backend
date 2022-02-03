import httpx
from functools import lru_cache


client = httpx.Client(timeout=30)


@lru_cache(4096)
def http_get(*args, **kwargs) -> httpx.Response:
    try:
        response = client.get(*args, **kwargs)
        response.raise_for_status()
        return response
    except httpx.RemoteProtocolError as e:
        response = client.get(*args, **kwargs)
        response.raise_for_status()
        return response
