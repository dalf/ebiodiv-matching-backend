from pathlib import Path

# matching algorithm: which columns
STRING_DISTANCE = [
    "family",
    "genus",
    "specificEpithet",
    "country",
    "recordedBy",
    "collectionCode",
    "catalogNumber",
]
INT_DISTANCE = ["year", "month", "individualCount"]
FLOAT_DISTANCE = ["decimalLongitude", "decimalLatitude", "elevation"]

# file names
root_path = Path(__file__).parent.parent
plazi_records = root_path / "input" / "Occurrences.clusteringPlaziRecords.csv"
output_path = root_path / "output"
institutions_db = output_path / "institutions.lmdb"
organizations_db = output_path / "organizations.lmdb"
datasetmetadata_db = output_path / "datasetmetadata.lmdb"
collections_db = output_path / "collections.lmdb"
occurrences_db = output_path / "occurrences.lmdb"
matcit_per_institutions_db = output_path / "matcit_per_institutions.lmdb"
