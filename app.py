import logging
from time import time
from typing import Dict, List, Optional, Union

import aiohttp
from fastapi import FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import diskmap
import matchingalgorithm
import server

MATCHING_DB = diskmap.DiskMap("matching.lmdb")
HTTP_SESSION: aiohttp.ClientSession = None
CONFIG = server.read_config("""
[server]
# see https://pgjones.gitlab.io/hypercorn/reference/source/hypercorn.config.html#hypercorn.config.Config
root_path=/
bind=localhost:8888

[datasource]
url=https://tb.plazi.org/GgServer/gbifOccLinkData/
timeout=180
""")
DATASOURCE = CONFIG["datasource"]

logger = logging.getLogger("ebiodiv")
logger_datasource = logger.getChild("datasource")
app = FastAPI(
    title="eBioDiv - Backend API",
    version="2.0.0",
    docs_url="/",
    redoc_url=None,
    swagger_ui_parameters={"syntaxHighlight": False},
    description="""
<p>See the <a href="https://candy.text-analytics.ch/eBioDiv/">project page</a></p>

<p>Front-end deployed at <a href='https://candy.text-analytics.ch/eBioDiv/demo/'>https://candy.text-analytics.ch/eBioDiv/demo/</a></p>

<p>This API mostly proxies the Plazi API at <a href="https://tb.plazi.org/GgServer/gbifOccLinkData/">https://tb.plazi.org/GgServer/gbifOccLinkData/</a>,
but add scoring between the occurrences</p>
""",
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("Exception")
        return JSONResponse(
            status_code=500,
            content={
                "error": exc.__class__.__module__ + "." + exc.__class__.__name__,
                "url": str(request.url),
                "args": exc.args,
            },
        )


app.middleware("http")(catch_exceptions_middleware)


async def on_request_end(session, trace_config_ctx, params):
    logger_datasource.info(f"\"{params.method} {params.url}\" {params.response.status} {params.response.headers.get('content-length', '')}")


@app.on_event("startup")
async def startup_event():
    global HTTP_SESSION
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_end.append(on_request_end)
    timeout = aiohttp.ClientTimeout(float(DATASOURCE["timeout"]))
    HTTP_SESSION = aiohttp.ClientSession(trace_configs=[trace_config], timeout=timeout)


@app.on_event("shutdown")
async def shutdown_event():
    await HTTP_SESSION.close()


class Fields(BaseModel):
    __root__: Dict[str, List[str]]


@app.get("/fields", response_model=Fields, description="List of fields", tags=["meta"])
async def get_fields():
    result = {column_name: [column_name] for column_name in matchingalgorithm.FIELDS}
    result.update({column_names[0]: list(column_names) for column_names in matchingalgorithm.MULTI_FIELDS})
    return result


@app.get("/institutionList", description="basic list of institutions, including datasets", tags=["data"])
async def get_institutionList():
    async with HTTP_SESSION.get(DATASOURCE["url"] + "institutionList") as response:
        return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])


@app.get("/institutions", description="list of full institution record", tags=["data"])
async def get_institutions():
    async with HTTP_SESSION.get(DATASOURCE["url"] + "institutions") as response:
        return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])


@app.get("/datasets", description="list of datasets", tags=["data"])
async def get_datasets(institutionKey: Optional[str] = None):
    params = {}
    if institutionKey:
        params["institutionKey"] = institutionKey
    async with HTTP_SESSION.get(DATASOURCE["url"] + "datasets", params=params) as response:
        return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])


def get_relation_id(occurrenceKey1, occurrenceKey2):
    relation_keys = [int(occurrenceKey1), int(occurrenceKey2)]
    relation_keys.sort()
    return str(relation_keys[0]) + "," + str(relation_keys[1])


def _add_score(data) -> None:
    # normalized a copy of the occurrences
    normalized_occ_dict = {}
    for occ_key, occ in data["occurrences"].items():
        normalized_occ = occ.copy()
        matchingalgorithm.normalize_occurrence(normalized_occ)
        normalized_occ_dict[int(occ_key)] = normalized_occ
    # get the scores from the normalized occurrences
    # leave the original occurrences untouched
    for relation in data["occurrenceRelations"]:
        o1 = normalized_occ_dict[relation["occurrenceKey1"]]
        o2 = normalized_occ_dict[relation["occurrenceKey2"]]
        relation["scores"] = matchingalgorithm.get_scores(o1, o2)


@app.get("/occurrences", description="list of occurrences", tags=["data"])
async def get_occurrences(institutionKey: Optional[str] = None, datasetKey: Optional[str] = None, occurrenceKeys: Optional[str] = None, scores: bool = False):
    params = {}
    if institutionKey is not None:
        params["institutionKey"] = institutionKey
    if datasetKey is not None:
        params["datasetKey"] = datasetKey
    if occurrenceKeys is not None:
        params["occurrenceKeys"] = occurrenceKeys
    async with HTTP_SESSION.get(DATASOURCE["url"] + "occurrences", params=params) as response:
        if response.status != 200:
            # error: proxy the response
            return Response(await response.read(), status_code=response.status, media_type=response.headers["Content-Type"])
        data = await response.json()

        # add matching
        for relation in data["occurrenceRelations"]:
            relation_id = get_relation_id(relation["occurrenceKey1"], relation["occurrenceKey2"])
            relation["matching"] = MATCHING_DB.get(relation_id, {"match": None, "timestamp": None, "comment": None})

        # add scores
        if scores:
            _add_score(data)
        return data


class OccurrenceMatching(BaseModel):
    occurrenceKey1: Union[str, int]
    occurrenceKey2: Union[str, int]
    match: bool = False
    comment: Optional[str] = None


@app.post("/matching", description='Update the "match" value between two occurrences', tags=["matching"])
async def add_matching(matchingInput: List[OccurrenceMatching]):
    now = int(time())
    response = []
    for item in matchingInput:
        relation_id = get_relation_id(item.occurrenceKey1, item.occurrenceKey2)
        data = {"match": item.match, "comment": item.comment, "timestamp": now}
        MATCHING_DB[relation_id] = data
        response.append({"occurrenceKey1": item.occurrenceKey1, "occurrenceKey2": item.occurrenceKey2, **data})
    return response


if __name__ == "__main__":
    server.run(CONFIG["server"], __name__ + ":app")
