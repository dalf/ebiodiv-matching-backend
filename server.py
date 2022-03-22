from typing import List
from datetime import datetime
from itertools import chain, islice

import orjson

import server_config
from parse import config
from parse.diskmap import DiskMap

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
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_compress import Compress
from flask_restplus import Namespace, Api, Resource, fields, reqparse


API_FIELDS = server_config.FIELDS + config.MATCHED_FIELDS


institutions = DiskMap.load(config.institutions_db)
matcit_per_institutions = DiskMap.load(config.matcit_per_institutions_db)
organizations = DiskMap(config.organizations_db, readonly=True)
datasetmetadata = DiskMap(config.datasetmetadata_db, readonly=True)
occurrences = DiskMap(config.occurrences_db, readonly=True, cache=False)

occurrences_to_dataId = {}
for dataId, value in matcit_per_institutions.items():
    for matcitKey in value["data"].keys():
        occurrences_to_dataId[matcitKey] = dataId


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


# API v1
blueprint = Blueprint("v1", __name__)
api_v1 = Api(blueprint)
app.register_blueprint(blueprint, url_prefix="/v1")
api_v1_browse = Namespace("Browse", description="get list of dataId")
api_v1_matching = Namespace("Matching", description="CRUD matching between material citations and specimens")
api_v1_data = Namespace(
    "Data",
    description="material citation and specimens for one institution (when found or a dataset when not found)",
)

api_v1.add_namespace(api_v1_browse, path="/browse")
api_v1.add_namespace(api_v1_data, path="/data")
api_v1.add_namespace(api_v1_matching, path="/matching")

CORS(app)
Compress(app)


def get_occurrences_fields():
    o_fields = {}
    for f in config.STRING_DISTANCE:
        o_fields[f] = fields.String()
    for f in config.INT_DISTANCE:
        o_fields[f] = fields.Integer()
    for f in config.FLOAT_DISTANCE:
        o_fields[f] = fields.Float()
    return o_fields


def get_score_fields():
    score_fields = {
        "mean": fields.Float(min=0, max=1, description="Average all of scores"),
        "string_mean": fields.Float(
            min=0,
            max=1,
            description="Average of the scores about string fields (to order the specimens)",
        ),
    }
    for f in config.STRING_DISTANCE:
        score_fields[f] = fields.Float(min=0, max=1)
    for f in config.INT_DISTANCE:
        score_fields[f] = fields.Float(min=0, max=1)
    for f in config.FLOAT_DISTANCE:
        score_fields[f] = fields.Float(min=0, max=1)
    return score_fields


model_score = api_v1.model("Score", {**get_score_fields()})

model_material_citation = api_v1.model(
    "MaterialCitation",
    {
        "key": fields.String(description="GBIF occurrence key"),
        "taxonKey": fields.String(description="Specie on GBIF: https://www.gbif.org/species/{taxonKey}/treatments"),
        "references": fields.String(
            description='Treatment on plazi if the value starts with "http://treatment.plazi.org"'
        ),
        **get_occurrences_fields(),
        "...": fields.Raw(
            description="all other fields from https://api.gbif.org/v1/occurrence/{key} (this is not a key)"
        ),
    },
)

model_specimen = api_v1.model(
    "Specimen",
    {
        "key": fields.String(description="GBIF occurrence key"),
        **get_occurrences_fields(),
        "...": fields.Raw(
            description="All other fields from https://api.gbif.org/v1/occurrence/{key} (this is not a key)"
        ),
        "$score": fields.Nested(
            model_score,
            description="1 = the specimen field is equal to the material citation field, 0 = different",
        ),
    },
)

model_matcit_specimen = api_v1.model(
    "MaterialCitationAndSpecimen",
    {
        "materialCitationOccurrence": fields.Nested(
            model_material_citation, description="Material citation (GBIF occurrence)"
        ),
        "institutionOccurrences": fields.Nested(model_specimen, description="Specimens to check (GBIF occurrences)"),
    },
)

model_matcit_dict = api_v1.model(
    "MaterialCitationAndSpecimenSet",
    {"<material citation GBIF key>": fields.Nested(model_matcit_specimen)},
)

model_dataset = api_v1.model(
    "DatasetDocument",
    {
        "eml:eml": fields.Nested(
            api_v1.model(
                "DatasetDocumentEml",
                {
                    "dataset": fields.Nested(
                        api_v1.model(
                            "DatasetDocumentEmlDataset",
                            {
                                "creator": fields.Raw(),
                                "abstract": fields.Raw(),
                                "alternateIdentifier": fields.List(
                                    fields.String(),
                                    description="List of identifiers, some are DOI, some are URL",
                                ),
                                "...": fields.Raw(
                                    description="All other fields from https://api.gbif.org/v1/dataset/{Specimen_datasetKey}/document (this is not a key)"
                                ),
                            },
                        )
                    )
                },
            )
        ),
    },
)

model_dataset_dict = api_v1.model("DatasetDocumentDict", {"<datasetKey>": fields.Nested(model_dataset)})

model_data = api_v1.model(
    "Data",
    {
        "key": fields.String("Key from /browse API endpoints"),
        "institutionKey": fields.String("institutionKey field from the specimen GBIF occurrences"),
        "institutionID": fields.String("institutionID field from the specimen GBIF occurrences"),
        "institutionCode": fields.String("institutionCode field from the specimen GBIF occurrences"),
        "publishingOrgKey": fields.String("publishingOrgKey field from the specimen GBIF occurrences"),
        "publishingOrg": fields.Raw(description="Content of https://api.gbif.org/v1/organization/{publishingOrgKey}"),
        "datasetDocuments": fields.Nested(
            model_dataset_dict,
            description="Content of https://api.gbif.org/v1/dataset/{Specimen_datasetKey}/document",
        ),
        "data": fields.Nested(model_matcit_dict),
    },
)

model_matching_matching = api_v1.model(
    "Matching",
    {
        "match": fields.Boolean("The curators has declared the matching done for this material citation"),
        "comment": fields.String("Optional comment"),
        "timestamp": fields.Integer("Timestamp of the last change"),
    },
)

model_matching_matcit = api_v1.model(
    "MatchingMaterialCitation",
    {
        "done": fields.Boolean("The curators has declared the matching done for this material citation"),
    },
)

model_matching_matcit_detail = api_v1.model(
    "MatchingMaterialCitationDetail",
    {
        "done": fields.Boolean("The curators has declared the matching done for this material citation"),
        "institutionOccurrences": fields.Nested(
            api_v1.model(
                "MatchingSpecimens",
                {"<specimenKey>": fields.Nested(model_matching_matching)},
            )
        ),
    },
)

model_matching_data = api_v1.model(
    "Data matching", {"<materialCitationKey>": fields.Nested(model_matching_matcit_detail)}
)


@api_v1.route("/fields")
@api_v1.doc(description="Fields of occurrences that are used by the matching algorithm.")
class api_columns(Resource):
    def get(self):
        return API_FIELDS


@api_v1_browse.route("/institutions")
@api_v1_browse.doc(description="Specimens with an institution that was found")
class api_key(Resource):
    @staticmethod
    def get_institution_extract(institutionKey):
        return {
            k: v
            for k, v in institutions[institutionKey].items()
            if k
            in (
                "additionalNames",
                "address",
                "alternativeCodees",
                "description",
                "homepage",
                "name",
            )
        }

    def get(self):
        return {
            key: api_key.get_institution_extract(key)
            for key, dataset in matcit_per_institutions.items()
            if dataset["institutionKey"] is not None
        }


class api_abstract_institution(Resource):
    @staticmethod
    def get_simple_dataset(key):
        prefix, datasetKey = key.split("_dataset_")
        if not datasetKey in datasetmetadata:
            return None
        result = {}

        if prefix.startswith("org_"):
            org = organizations[prefix[4:]]
            result["organization"] = {k: v for k, v in org.items() if k in ("key", "descriptions", "title")}

        if prefix.startswith("id_"):
            institutionCodeAndID = prefix[4:].split("_")
            if len(institutionCodeAndID) == 2:
                result["institutionCode"], result["institutionID"] = (
                    institutionCodeAndID[0],
                    institutionCodeAndID[1],
                )

        metadata = datasetmetadata[datasetKey].get("eml:eml", {}).get("dataset", {})
        result["dataset"] = {
            k: v
            for k, v in metadata.items()
            if k
            in (
                "alternateIdentifier",
                "title",
                "abstract",
                "purpose",
                "creator",
                "associatedParty",
            )
        }
        result["dataset"]["key"] = datasetKey
        return result


@api_v1_browse.route("/unknown_institutions")
@api_v1_browse.doc(
    description="Specimens with an institution code or id that is not found. Return information about the dataset ( https://api.gbif.org/v1/dataset/{datasetKey}/document ) and the organization ( https://api.gbif.org/v1/organization/{publishingOrgKey} )"
)
class api_unkown_institutions(api_abstract_institution):
    def get(self):
        return {
            key: api_unkown_institutions.get_simple_dataset(key)
            for key, dataset in matcit_per_institutions.items()
            if dataset["institutionKey"] is None and (dataset["institutionCode"] or dataset["institutionID"])
        }


@api_v1_browse.route("/no_institutions")
@api_v1_browse.doc(
    description="Specimens without reference to an institutions. Return information about the dataset ( https://api.gbif.org/v1/dataset/{datasetKey}/document )"
)
class api_no_institutions(api_abstract_institution):
    def get(self):
        return {
            key: api_unkown_institutions.get_simple_dataset(key)
            for key, dataset in matcit_per_institutions.items()
            if dataset["institutionKey"] is None
            and dataset["institutionCode"] is None
            and dataset["institutionID"] is None
        }


def get_occurrences_api(occ):
    return {
        k: v
        for k, v in occ.items()
        if k in API_FIELDS
    }


def get_matcit_specimen(data):
    return {
        key: {
            "materialCitationOccurrence": get_occurrences_api(occurrences[key]),
            "institutionOccurrences": {
                ik: { **get_occurrences_api(occurrences[str(ik)]), "$score": vk} for ik, vk in value["institutionOccurrences"].items()
            },
        }
        for key, value in data.items()
    }


def get_specimen_matcit(data):
    result = {}
    for matcitKey, institutionOccurences in data.items():
        matcitOccurrence = get_occurrences_api(occurrences[matcitKey])
        for institutionKey, score in institutionOccurences['institutionOccurrences'].items():
            institutionKey = str(institutionKey)
            if institutionKey not in result:
                result[institutionKey] = {
                    "institutionOccurrence": get_occurrences_api(occurrences[institutionKey]),
                    "materialCitationOccurrences": {},
                }
            result[institutionKey]["materialCitationOccurrences"][matcitKey] = {
                **matcitOccurrence,
                "$score": score,
            }
    return result


class DataFormatType(fields.String):
    '''Restrict input to an integer in a range (inclusive)'''

    def __call__(self, value):
        return value

    @property
    def __schema__(self):
        return {
            'type': 'string',
            'enum': ['matcit_specimen', 'specimen_matcit'],
        }


data_format_parser = reqparse.RequestParser()
data_format_parser.add_argument('format', type=DataFormatType(), help='matcit_specimen or specimen_matcit')


@api_v1_data.route("/<dataId>")
@api_v1_data.param(
    "dataId",
    "Identifier from /browse/institutions or /browse/no_institutions or /browse/unknown_institutions",
)
class api_data_list(Resource):
    @api_v1_data.response(404, "Not found")
    @api_v1_data.response(500, "Internal error")
    @api_v1_data.doc(model=model_data)
    @api_v1_data.expect(data_format_parser)
    def get(self, dataId):
        if dataId not in matcit_per_institutions:
            response = jsonify({"error": "Not found"})
            response.status_code = 404
            return response

        args = data_format_parser.parse_args()
        format = args.get("format") or "matcit_specimen"

        try:
            data = {**matcit_per_institutions[dataId]}
            data["publishingOrg"] = organizations.get(data["publishingOrgKey"])
            data["institution"] = institutions.get(data["institutionKey"])
            data["datasetDocuments"] = {key: datasetmetadata[key] for key in data["datasetDocuments"]}
            if format == "matcit_specimen":
                data["data"] = get_matcit_specimen(data["data"])
            elif format == "specimen_matcit":
                data["data"] = get_specimen_matcit(data["data"])
            response = make_response(orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))
            response.content_type = app.config["JSONIFY_MIMETYPE"]
            return response
        except FileNotFoundError:
            response = jsonify({"error": "Not found"})
            response.status_code = 404
            return response


def chunked(seq, chunksize):
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:
        try:
            yield list(chain([next(it)], islice(it, chunksize-1)))
        except StopIteration:
            break


@api_v1_matching.route("/data/<dataId>")
@api_v1_matching.doc(description=r'For each material citation of \<dataId\>, returns the "done" status')
@api_v1_matching.param(
    "dataId",
    "Identifier from /browse/institutions or /browse/no_institutions or /browse/unknown_institutions",
)
class api_matching_data(Resource):
    @api_v1_data.doc(model=model_matching_data)
    def get(self, dataId):
        if dataId not in matcit_per_institutions:
            response = jsonify({"error": "Not found"})
            response.status_code = 404
            return response

        data = {**matcit_per_institutions[dataId]}
        materialCitationKeyList = data["data"].keys()
        output = {
            str(k): {
                "done": False,
                "institutionOccurrences": {
                    str(institutionOcc): {"match": None, "comment": None, "timestamp": None}
                    for institutionOcc in v["institutionOccurrences"].keys()
                },
            }
            for k, v in data["data"].items()
        }
        # avoid sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) too many SQL variables
        # see https://www.sqlite.org/limits.html : sqlite doesn't allow more than 999
        # zip iter trick : https://stackoverflow.com/questions/1335392/iteration-over-list-slices
        for matCitKeySubList in chunked(iter(materialCitationKeyList), 900):
            result = db.session.execute(
                select(MaterialCitation).where(MaterialCitation.materialCitationKey.in_(matCitKeySubList))
            ).all()
            for item in result:
                output[str(item[0].materialCitationKey)]["done"] = item[0].done
            result: List[Matching] = db.session.execute(
                select(Matching).where(Matching.materialCitationKey.in_(matCitKeySubList))
            ).all()
            for item in result:
                item0 = item[0]
                o = output[str(item0.materialCitationKey)]["institutionOccurrences"].setdefault(
                    str(item0.specimenKey), {"not_related": True}
                )
                o.update(
                    {
                        "match": item0.match,
                        "comment": item0.comment,
                        "timestamp": int(item0.timestamp.timestamp()),
                    }
                )
        return jsonify(output)


@api_v1_matching.route("/materialcitation/<materialCitationKey>")
@api_v1_data.param(
    "materialCitationKey",
    "materialCitationKey found in /data/{dataId}",
)
class api_matching_matcit(Resource):
    @api_v1_matching.doc(
        model=model_matching_matcit_detail,
        description='Get the "done" status for one material citation',
    )
    @api_v1_matching.response(404, "Not found")
    @api_v1_matching.response(500, "Internal error")
    def get(self, materialCitationKey):
        result = db.session.execute(
            select(MaterialCitation).where(MaterialCitation.materialCitationKey == materialCitationKey)
        ).scalar()
        output = {"done": False, "specimens": {}}
        if result:
            output["done"] = result.done
        result: List[Matching] = db.session.execute(
            select(Matching).where(Matching.materialCitationKey == materialCitationKey)
        ).all()
        for item in result:
            item0 = item[0]
            output["specimens"][item0.specimenKey] = {
                "match": item0.match,
                "comment": item0.comment,
                "timestamp": int(item0.timestamp.timestamp()),
            }
        return jsonify(output)

    @api_v1_data.response(500, "Internal error")
    @api_v1_data.doc(
        body=model_matching_matcit,
        description='Update the "done" status for one material citation',
    )
    def put(self, materialCitationKey):
        m = MaterialCitation(materialCitationKey=materialCitationKey, **request.json)
        db.session.execute(delete(MaterialCitation).where(MaterialCitation.materialCitationKey == materialCitationKey))
        db.session.add(m)
        db.session.commit()
        return jsonify({ 'status': 'ok'})

    @api_v1_data.response(500, "Internal error")
    @api_v1_data.doc(
        body=model_matching_matcit_detail,
        description='Bulk update: update the "done" status for one material citation and all related specimens. Previous records are deleted.',
    )
    def post(self, materialCitationKey):
        body = request.json
        if not body:
            return "Bad request", 400
        db.session.execute(delete(MaterialCitation).where(MaterialCitation.materialCitationKey == materialCitationKey))
        db.session.execute(delete(Matching).where(Matching.materialCitationKey == materialCitationKey))
        for specimenKey, specimen in body["specimens"].items():
            m = Matching(
                materialCitationKey=materialCitationKey,
                specimenKey=specimenKey,
                **specimen,
            )
            db.session.add(m)
        m = MaterialCitation(materialCitationKey=materialCitationKey, done=body["done"])
        db.session.add(m)
        db.session.commit()
        return jsonify({ 'status': 'ok'})


@api_v1_matching.route("/materialcitation/<materialCitationKey>/specimen/<specimenKey>")
class api_matching_matcit_specimen(Resource):
    def _check_parameters(self, materialCitationKey, specimenKey):
        if materialCitationKey not in occurrences_to_dataId:
            return "materialCitationKey doesn't exist"
        specimens = matcit_per_institutions[occurrences_to_dataId[materialCitationKey]]["data"][materialCitationKey]['institutionOccurrences']
        if specimenKey not in specimens:
            return "specimenKey is not related to materialCitationKey"
        return None

    @api_v1_matching.doc(
        model=model_matching_matching,
        description='Get the "match" value between a material citation and a specimen',
    )
    @api_v1_matching.response(404, "Not found")
    @api_v1_matching.response(500, "Internal error")
    def get(self, materialCitationKey, specimenKey):
        errorMsg = self._check_parameters(materialCitationKey, specimenKey)
        if errorMsg:
            return errorMsg, 404

        result = db.session.execute(
            select(Matching).where(
                Matching.materialCitationKey == materialCitationKey and Matching.specimenKey == specimenKey
            )
        ).scalar()
        if not result:
            response = jsonify({})
            response.status_code = 404
            return response
        return jsonify(
            {
                "materialCitationKey": str(result.materialCitationKey),
                "specimenKey": str(result.specimenKey),
                "match": result.match,
                "comment": result.comment,
                "timestamp": int(result.timestamp.timestamp()),
            }
        )

    @api_v1_matching.doc(
        body=model_matching_matching,
        description='Update the "match" value between a material citation and a specimen',
    )
    @api_v1_matching.response(500, "Internal error")
    def put(self, materialCitationKey, specimenKey):
        errorMsg = self._check_parameters(materialCitationKey, specimenKey)
        if errorMsg:
            return errorMsg, 404

        m = Matching(
            materialCitationKey=materialCitationKey,
            specimenKey=specimenKey,
            **request.json,
        )
        m.timestamp = datetime.now()
        db.session.execute(
            delete(Matching).where(
                Matching.materialCitationKey == materialCitationKey and Matching.specimenKey == specimenKey
            )
        )
        db.session.add(m)
        db.session.commit()

    @api_v1_matching.doc(
        body=model_matching_matching,
        description='Delete the "match" value between a material citation and a specimen',
    )
    @api_v1_matching.response(500, "Internal error")
    def delete(self, materialCitationKey, specimenKey):
        errorMsg = self._check_parameters(materialCitationKey, specimenKey)
        if errorMsg:
            return errorMsg, 404

        db.session.execute(
            delete(Matching).where(
                Matching.materialCitationKey == materialCitationKey and Matching.specimenKey == specimenKey
            )
        )
        db.session.commit()


@app.route("/")
def index():
    return redirect("/v1", 308)


if __name__ == "__main__":
    app.run(
        debug=APP_DEBUG,
        use_debugger=APP_DEBUG,
        threaded=True,
        host="127.0.0.1",
        port=8888,
    )
