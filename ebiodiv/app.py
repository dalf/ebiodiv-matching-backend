import logging
import asyncio
import multiprocessing
from time import time
from itertools import chain, islice
from typing import Dict, List, Optional, Union

import orjson
import aiohttp
from fastapi import FastAPI, Request, Response, Body, Query
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
from logging import Logger

from . import diskmap, matchingalgorithm, server, utils


logger = logging.getLogger(__name__)

CONFIG = server.CONFIG
MATCHING_DB = diskmap.DiskMap("matching.lmdb")
HTTP_SESSION: aiohttp.ClientSession = None
DATASOURCE = CONFIG["datasource"]

app = FastAPI(
    title="eBioDiv - Backend API",
    version="2.0.0",
    docs_url="/",
    redoc_url=None,
    default_response_class=ORJSONResponse,
    swagger_ui_parameters={"syntaxHighlight": False},
    description="""
<p>See the <a href="https://candy.text-analytics.ch/eBioDiv/">project page</a></p>

<p>Front-end deployed at <a href='https://candy.text-analytics.ch/eBioDiv/demo/'>https://candy.text-analytics.ch/eBioDiv/demo/</a></p>

<p>This API mostly proxies the Plazi API at <a href="https://tb.plazi.org/GgServer/gbifOccLinkData/">https://tb.plazi.org/GgServer/gbifOccLinkData/</a>,
but add scoring between the occurrences</p>
""",
)


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("Exception")
        return ORJSONResponse(
            status_code=500,
            content={
                "error": exc.__class__.__module__ + "." + exc.__class__.__name__,
                "url": str(request.url),
                "args": exc.args,
            },
        )


app.middleware("http")(catch_exceptions_middleware)

app.add_middleware(
    GZipMiddleware,
    minimum_size=1000
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

server.configure_app(app)

async def on_request_end(session, trace_config_ctx, params):
    logger.info(f"\"{params.method} {params.url}\" {params.response.status} {params.response.headers.get('content-length', '')}")


@app.on_event("startup")
async def startup_event():
    """create HTTP client & log outgoing HTTP request"""
    global HTTP_SESSION
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_end.append(on_request_end)
    timeout = aiohttp.ClientTimeout(float(DATASOURCE["timeout"]))
    HTTP_SESSION = aiohttp.ClientSession(trace_configs=[trace_config], timeout=timeout, headers={
        'User-Agent': 'ebiodiv-backend'
    })


@app.on_event("shutdown")
async def shutdown_event():
    await HTTP_SESSION.close()


class Fields(BaseModel):
    __root__: Dict[str, List[str]]


async def proxy_response(url, **kwargs):
    with utils.measure_time() as now:
        async with HTTP_SESSION.get(url, **kwargs) as response:
            content = await response.read()
            http_time = now()
            return Response(
                content,
                status_code=response.status,
                media_type=response.headers["Content-Type"],
                headers = {
                    'server-timing': 'http;dur=' + str(http_time * 1000)
                }
            )


@app.get("/fields", response_model=Fields, description="List of fields", tags=["meta"])
async def get_fields():
    result = {column_name: [column_name] for column_name in matchingalgorithm.FIELDS}
    result.update({column_names[0]: list(column_names) for column_names in matchingalgorithm.MULTI_FIELDS})
    return result


@app.get("/institutionList", description="basic list of institutions, including datasets", tags=["data"])
async def get_institutionList():
    return await proxy_response(DATASOURCE["url"] + "institutionList")


@app.get("/institutions", description="list of full institution record", tags=["data"])
async def get_institutions():
    return await proxy_response(DATASOURCE["url"] + "institutions")


@app.get("/datasets", description="list of datasets", tags=["data"])
async def get_datasets(institutionKey: Optional[str] = None):
    params = {}
    if institutionKey:
        params["institutionKey"] = institutionKey
    return await proxy_response(DATASOURCE["url"] + "datasets", params=params)


def get_relation_id(occurrenceKey1, occurrenceKey2):
    relation_keys = [int(occurrenceKey1), int(occurrenceKey2)]
    relation_keys.sort()
    return str(relation_keys[0]) + "," + str(relation_keys[1])


def _add_score_on_chunk(normalized_occ_dict, chunk: List[Dict]) -> List[Dict]:
    # get the scores from the normalized occurrences
    # leave the original occurrences untouched
    for relation in chunk:
        o1 = normalized_occ_dict[relation["occurrenceKey1"]]
        o2 = normalized_occ_dict[relation["occurrenceKey2"]]
        relation["scores"] = matchingalgorithm.get_scores(o1, o2)
    return chunk


def _add_score(data) -> None:
    # normalized a copy of the occurrences
    normalized_occ_dict = {}
    for occ_key, occ in data["occurrences"].items():
        normalized_occ = occ.copy()
        matchingalgorithm.normalize_occurrence(normalized_occ)
        normalized_occ_dict[int(occ_key)] = normalized_occ

    # few relations: sync call
    if len(data["occurrenceRelations"]) < 200 or True:
        _add_score_on_chunk(normalized_occ_dict, data["occurrenceRelations"])
        return

    # a lot of relations: use a process pool
    chunk_size = max(100, int(len(data["occurrenceRelations"]) / utils.get_worker_count()) + 1)
    chunks = utils.chunked(data["occurrenceRelations"], chunk_size)
    data["occurrenceRelations"] = utils.pool_map(_add_score_on_chunk, chunks, normalized_occ_dict)


@app.get("/occurrences", description="list of occurrences", tags=["data"])
async def get_occurrences(
    institutionKey: Optional[str] = None,
    datasetKey: Optional[str] = None,
    occurrenceKeys: Optional[str] = None,
    fetchMissing: Optional[bool] = Query(default=None, description="Fetch missing occurrences, allow to add new occurrences"),
    scores: bool = False
):
    params = {}
    if institutionKey is not None:
        params["institutionKey"] = institutionKey
    if datasetKey is not None:
        params["datasetKey"] = datasetKey
    if occurrenceKeys is not None:
        params["occurrenceKeys"] = occurrenceKeys
    if fetchMissing is not None:
        params["fetchMissing"] = "true" if fetchMissing else "false"

    timings = {}

    with utils.measure_time() as now:
        async with HTTP_SESSION.get(DATASOURCE["url"] + "occurrences", params=params) as response:
            if response.status != 200:
                # error: proxy the response
                return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])

            content = await response.read()
    timings['http'] = now()

    with utils.measure_time() as now:
        # orjson.loads(content) takes a few seconds on a large documents (>10MB).
        data = orjson.loads(content)
    timings['json_loads'] = now()

    # add matching
    for relation in data["occurrenceRelations"]:
        relation_id = get_relation_id(relation["occurrenceKey1"], relation["occurrenceKey2"])
        relation["matching"] = MATCHING_DB.get(relation_id, {"match": None, "timestamp": None, "comment": None})

    # add scores
    with utils.measure_time() as now:
        if scores:
            await asyncio.get_event_loop().run_in_executor(None, _add_score, data)
    timings['scoring'] = now()

    # serialize JSON
    with utils.measure_time() as now:
        content = orjson.dumps(data)
    timings['json_dumps'] = now()

    # output
    timings_values = [
        name + ';dur=' + str(round(value * 1000, 3))
        for name, value in timings.items()
    ]

    return Response(
        orjson.dumps(data),
        status_code=response.status,
        media_type="application/json",
        headers = {
            'server-timing': ', '.join(timings_values)
        }
    )


class OccurrenceMatching(BaseModel):
    occurrenceKey1: Union[str, int]
    occurrenceKey2: Union[str, int]
    match: bool = False
    comment: Optional[str] = None


@app.post("/matching", description='Update the "match" value between two occurrences. ⚠️ DEPRECATED use /occurrenceRelations', tags=["matching"])
async def add_matching(matchingInput: List[OccurrenceMatching]):
    now = int(time())
    response = []
    for item in matchingInput:
        relation_id = get_relation_id(item.occurrenceKey1, item.occurrenceKey2)
        data = {"match": item.match, "comment": item.comment, "timestamp": now}
        MATCHING_DB[relation_id] = data
        response.append({"occurrenceKey1": item.occurrenceKey1, "occurrenceKey2": item.occurrenceKey2, **data})
    return response


@app.post("/occurrenceRelations", description='Update the "match" value between occurrences', tags=["matching"])
async def occurrence_relations(data = Body(default=None, example="""{"body":{"occurrenceRelations":[{"occurrenceKey1":20,"occurrenceKey2":42,"decision":null},{"occurrenceKey1":20,"occurrenceKey2":42,"decision":true},{"occurrenceKey1":20,"occurrenceKey2":42,"decision":false}]}}""")):
    async with HTTP_SESSION.post(DATASOURCE["url"] + "occurrenceRelations", json=data) as response:
        return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])
