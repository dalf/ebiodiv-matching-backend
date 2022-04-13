from email.policy import default
from typing import List, Optional
from datetime import date
from time import time
from itertools import chain, islice

from pydantic import BaseModel, ValidationError
import httpx

import server_config
import matchingalgorithm
import diskmap

import flaskfix

from flask import Flask, jsonify, Blueprint, request, redirect, make_response
from flask.wrappers import Response
from flask_cors import CORS
from flask_compress import Compress
from flask_restplus import Api, Resource, Namespace, fields, reqparse
from flask_restplus import inputs


APP_DEBUG = True
app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
app.config["RESTPLUS_MASK_SWAGGER"] = False
if server_config.BASE_URL:
    flaskfix.patch_application(app, server_config.BASE_URL)


matching_db = diskmap.DiskMap('matching.lmdb')

class MatchingDecision(BaseModel):
    match: Optional[bool] = None
    timestamp: Optional[int] = None
    comment: Optional[str] = None


# API v2
blueprint = Blueprint("v2", __name__)
api_v2 = Api(blueprint)
app.register_blueprint(blueprint, url_prefix="/v2")

api_v2_matching = Namespace("Matching", description="read & update the matching beetween the occurrences")
api_v2_meta = Namespace("Meta", description="meta information")

api_v2.add_namespace(api_v2_matching, path="/matching")
api_v2.add_namespace(api_v2_meta, path="/meta")

CORS(app)
Compress(app)


# model
model_post_matching = api_v2.model(
    "Matching",
    {
        "occurrenceKey1": fields.String("occurrence key 1"),
        "occurrenceKey2": fields.String("occurrence key 2"),
        "match": fields.Boolean("The curators has declared the matching done for this material citation"),
        "comment": fields.String("Optional comment"),
    },
)


def log_response(response: httpx.Response):
    print(response.url)


HTTPCLIENT = httpx.Client(
    base_url=server_config.DATASOURCE_URL,
    timeout=server_config.DATASOURCE_TIMEOUT,
    event_hooks={'response': [log_response]},
)


########

datasets_format_parser = reqparse.RequestParser()
datasets_format_parser.add_argument('institutionKey', type=str, help='institutionKey')

########

occurrences_format_parser = reqparse.RequestParser()
occurrences_format_parser.add_argument('institutionKey', type=str, help='institutionKey')
occurrences_format_parser.add_argument('datasetKey', type=str, help='datasetKey')
occurrences_format_parser.add_argument('occurrenceKeys', type=str, help='list of occurrenceKey separated by space')
occurrences_format_parser.add_argument('scores', type=inputs.boolean, default=False, help='should includes the scores?')

########

matching_get_parser = reqparse.RequestParser()
matching_get_parser.add_argument('occurrenceKey1', type=str, help='first occurrence key')
matching_get_parser.add_argument('occurrenceKey2', type=str, help='second occurrence key')

########

# fields
@api_v2_meta.route("/fields")
@api_v2_meta.doc(description="fields")
class fieldList(Resource):
    def get(self):
        result = {
            column_name: [ column_name ]
            for column_name in matchingalgorithm.FIELDS
        }
        result.update({
            column_names[0]: list(column_names)
            for column_names in matchingalgorithm.MULTI_FIELDS
        })
        return result


# institutionList
@api_v2.route("/institutionList")
@api_v2.doc(description="basic list of institutions, including datasets")
class institutionList(Resource):
    def get(self):
        return HTTPCLIENT.get("/institutionList").json()


# institutions
@api_v2.route("/institutions")
@api_v2.doc(description="list of full institution record")
class institutions(Resource):
    def get(self):
        response = HTTPCLIENT.get("/institutions")
        return Response(response.content, status=response.status_code, content_type=response.headers['Content-Type'])


# datasets
@api_v2.route("/datasets")
@api_v2.doc(description="list of datasets")
class datasets(Resource):
    @api_v2.expect(datasets_format_parser)
    def get(self):
        args = datasets_format_parser.parse_args()
        institutionKey = args.get("institutionKey")
        if institutionKey is None:
            return HTTPCLIENT.get("/datasets").json()
        response = HTTPCLIENT.get(f"/datasets?institutionKey={institutionKey}")
        return Response(response.content, status=response.status_code, content_type=response.headers['Content-Type'])


def chunked(seq, chunksize):
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:
        try:
            yield list(chain([next(it)], islice(it, chunksize-1)))
        except StopIteration:
            break


def get_relation_id(occurrenceKey1, occurrenceKey2):
    relation_keys = [
        int(occurrenceKey1),
        int(occurrenceKey2)
    ]
    relation_keys.sort()
    return str(relation_keys[0]) + ',' + str(relation_keys[1]) 


# occurrences
@api_v2.route("/occurrences")
@api_v2.doc(description="list of occurrences")
class occurrences(Resource):

    def _add_score(self, data) -> None:
        # normalized a copy of the occurrences
        normalized_occ_dict = {}
        for occ_key, occ in data['occurrences'].items():
            normalized_occ = occ.copy()
            matchingalgorithm.normalize_occurrence(normalized_occ)
            normalized_occ_dict[int(occ_key)] = normalized_occ
        # get the scores from the normalized occurrences
        # leave the original occurrences untouched
        for relation in data['occurrenceRelations']:
            o1 = normalized_occ_dict[relation['occurrenceKey1']]
            o2 = normalized_occ_dict[relation['occurrenceKey2']]
            relation['scores'] = matchingalgorithm.get_scores(o1, o2)
    
    def _add_matching(self, data) -> None:
        for relation in data['occurrenceRelations']:
            relation_id = get_relation_id(relation['occurrenceKey1'], relation['occurrenceKey2'])
            relation['matching'] = matching_db.get(relation_id, {
                'match': None,
                'timestamp': None,
                'comment': None
            })

    @api_v2.expect(occurrences_format_parser)
    def get(self):
        params = occurrences_format_parser.parse_args()
        include_scores = params.pop('scores')
        for k, v in list(params.items()):
            if v is None:
                del params[k]
        response = HTTPCLIENT.get(f"/occurrences", params=params)
        if response.status_code != 200:
            return Response(response.content, status=response.status_code, content_type=response.headers['Content-Type'])
        data = response.json()
        if include_scores:
            self._add_score(data)
            self._add_matching(data)
        return jsonify(data)


@api_v2_matching.route("")
@api_v2_matching.doc(description="list of occurrences")
class matching(Resource):

    @api_v2.doc(description='Get the "match" value between two occurrences',)
    @api_v2.response(400, "Bad request")
    @api_v2.response(500, "Internal error")
    @api_v2.expect(matching_get_parser)
    def get(self):
        params = matching_get_parser.parse_args()
        occurrenceKey1 = params.get('occurrenceKey1')
        occurrenceKey2 = params.get('occurrenceKey2')
        relation_id = get_relation_id(occurrenceKey1, occurrenceKey2)
        matching_decision = matching_db.get(relation_id, {})
        matching_decision = MatchingDecision(**matching_decision).dict()
        matching_decision['occurrenceKey1'] = int(occurrenceKey1)
        matching_decision['occurrenceKey2'] = int(occurrenceKey2)
        return matching_decision

    @api_v2.doc(
        body=model_post_matching,
        description='Update the "match" value between two occurrences',
    )
    @api_v2.response(400, "Bad request")
    @api_v2.response(500, "Internal error")
    def post(self):
        decission = request.json or {}
        decission['timestamp'] = time()
        occurrenceKey1 = decission.get('occurrenceKey1')
        occurrenceKey2 = decission.get('occurrenceKey2')
        if not occurrenceKey1 or not occurrenceKey2:
            return {
                "errors": "occurrenceKey1 and/or occurrenceKey2 are missing"
            }, 400
        relation_id = get_relation_id(decission['occurrenceKey1'], decission['occurrenceKey2'])
        try:
            matching_decision = MatchingDecision(**request.json).dict()
        except ValidationError as e:
            return {
                "errors": e.errors()
            }, 400
        else:
            matching_db[relation_id] = matching_decision
            matching_decision['occurrenceKey1'] = int(decission['occurrenceKey1'])
            matching_decision['occurrenceKey2'] = int(decission['occurrenceKey2'])
            return matching_db[relation_id]


@app.route("/")
def index():
    return redirect("/v2", 308)


if __name__ == "__main__":
    app.run(
        debug=APP_DEBUG,
        use_debugger=APP_DEBUG,
        threaded=True,
        host="127.0.0.1",
        port=8888,
    )
