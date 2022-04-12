from email.policy import default
from typing import List
from datetime import datetime

import orjson
import httpx

import server_config
import matchingalgorithm

from sqlalchemy import (
    select,
    delete,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
)

import flaskfix

from flask import Flask, jsonify, Blueprint, request, redirect, make_response
from flask.wrappers import Response
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_compress import Compress
from flask_restplus import Api, Resource, fields, reqparse
from flask_restplus import inputs


APP_DEBUG = True
app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
app.config["SQLALCHEMY_DATABASE_URI"] = server_config.DATABASE_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
app.config["RESTPLUS_MASK_SWAGGER"] = False
if server_config.BASE_URL:
    flaskfix.patch_application(app, server_config.BASE_URL)


# database
class MaterialCitation(db.Model):
    __tablename__ = "materialcitation"

    materialCitationKey = Column(String, primary_key=True)
    done = Column(Boolean, nullable=False)


class Matching(db.Model):
    __tablename__ = "matching"
    # __table_args__ = (UniqueConstraint('materialCitationKey', 'specimenKey', name='_materialCitationKey_specimenKey_uc'), )

    _id = Column(Integer, primary_key=True)
    materialCitationKey = Column(Integer, nullable=False)
    specimenKey = Column(String, nullable=False)
    match = Column(Boolean, nullable=True)
    comment = Column(String, nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)


db.create_all()


# API v2
blueprint = Blueprint("v2", __name__)
api_v2 = Api(blueprint)
app.register_blueprint(blueprint, url_prefix="/v2")

CORS(app)
Compress(app)


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

# fields
@api_v2.route("/fields")
@api_v2.doc(description="fields")
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
        return jsonify(data)


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
