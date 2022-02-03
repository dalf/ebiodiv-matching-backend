# server

```
pip install -r requirements.txt
waitress-serve server:app
```

Use `server.ini` to configured the server:

```ini
[server]
base_url=/

[database]
uri=sqlite:///matching.db
```

# parsing

See `output/requirements.txt` to install the Python dependencies

parse/extract1.py
* input: input/Occurrences.clusteringPlaziRecords.csv
* output: output/occurrences.lmdb

parse/extract2.py
* input:
    * input/Occurrences.clusteringPlaziRecords.csv
    * output/occurrences.lmdb
* output:
    * output/institutions.lmdb
    * output/datasetmetadata.lmdb

parse/extract3.py
* input:
    * input/Occurrences.clusteringPlaziRecords.csv
    * output/occurrences.lmdb
* output:
    * output/institutions.json
    * output/datasetmetadata.json

parse/make_cluster.py
* input:
    * input/Occurrences.clusteringPlaziRecords.csv
    * output/occurrences.json
    * output/datasetmetadata.json
    * output/organizations.json
* output:
    * output/matcit_per_institutions.lmdb

# output

## how occurrences are clustered
* by institution, read from (first match):
    * the `institutionKey`
    * the `institutionCode` if and only if the code is unique (using https://api.gbif.org/v1/grscicoll/institution?code=xxx )
    * the `institutionID` if and only if the id is unique (using https://api.gbif.org/v1/grscicoll/institution?identifier=xxx )
* the institution is not found:
	* `"id_" + institutionID + "_" + institutionCode + "_dataset" + datasetKey`
	* or `"org_" + publishingOrgKey + "_dataset" + datasetKey`
