import csv
import statistics
from multiprocessing import Pool, cpu_count

from tqdm import tqdm

import matchingalgorithm
import config
from diskmap import DiskMap

## load data
matcit_per_institutions = DiskMap(config.matcit_per_institutions_db)
occurrences = DiskMap.load(config.occurrences_db)
print(len(occurrences))

organizations = DiskMap(config.organizations_db, readonly=True)
datasetmetadata = DiskMap(config.datasetmetadata_db, readonly=True)

## load data: institutions
institutions = DiskMap(config.institutions_db, readonly=True)
institutions_per_code = dict()
institutions_per_id = dict()
for institution in institutions.values():
    if "code" in institution:
        institutions_per_code[institution["code"]] = institution
    for identifier in institution.get("identifiers", []):
        institutions_per_id[identifier["identifier"]] = institution

## load data: collections
collections = DiskMap(config.collections_db, readonly=True)
collections_per_code = dict()
collections_per_id = dict()
for collection in collections.values():
    if "code" in collection:
        collections_per_code[collection["code"]] = collection
    for identifier in collection.get("identifiers", []):
        collections_per_id[identifier["identifier"]] = collection


rows = []
print(f"read {config.plazi_records}")
with open(config.plazi_records) as csvfile:
    occurence_reader = csv.reader(csvfile, delimiter=";", quotechar="|")
    count = 0
    for row in tqdm(occurence_reader):
        count += 1
        if count > 1:
            rows.append([row[0], row[1]])


def normalize_name(name):
    for c in ["/", "\\", "<", ">", ":", '"', "|", "?", "*"]:
        name = name.replace(c, "-")
    return name


def get_institutionKey(occurrence):
    if "institutionKey" in occurrence:
        return occurrence["institutionKey"], True

    if "institutionCode" in occurrence:
        institutionCode = occurrence["institutionCode"]
        if institutionCode in institutions_per_code:
            return institutions_per_code[institutionCode]["key"], True

    if "institutionID" in occurrence:
        institutionID = occurrence["institutionID"]
        if institutionID in institutions_per_id:
            return institutions_per_id[institutionID]["key"], True

    """
    if "collectionKey" in occurrence:
        collection = collections[occurrence["collectionKey"]]
        if "institutionKey" in collection:
            return collection["institutionKey"], True

    if "collectionCode" in occurrence:
        collection = collections_per_code.get(occurrence["collectionCode"])
        if collection and "institutionKey" in collection:
            return collection["institutionKey"], True

    if "collectionID" in occurrence:
        collection = collections_per_id.get(occurrence["collectionID"])
        if collection and "institutionKey" in collection:
            return collection["institutionKey"], True
    """

    if "institutionCode" in occurrence or "institutionID" in occurrence:
        # fallback to code & id & dataset key
        return (
            "id__"
            + normalize_name(occurrence.get("institutionCode", ""))
            + "_"
            + normalize_name(occurrence.get("institutionID", ""))
            + "_dataset_"
            + occurrence.get("datasetKey", "unknown"),
            False,
        )

    # fallback to publishing organization & dataset key
    return (
        "org_"
        + occurrence.get("publishingOrgKey", "unknown")
        + "_dataset_"
        + occurrence.get("datasetKey", "unknown"),
        False,
    )


def create_clusters(occurrences):
    clusterDict = dict()
    rows = []
    with open(config.plazi_records) as csvfile:
        occurence_reader = csv.reader(csvfile, delimiter=";")
        next(occurence_reader)
        for row in tqdm(occurence_reader):
            institutionOccurrenceId = row[1]
            materialCitationOccurrenceId = row[0]
            rows.append((institutionOccurrenceId, materialCitationOccurrenceId))

            institutionOccurrence = occurrences[institutionOccurrenceId]
            materialCitationOccurrence = occurrences[materialCitationOccurrenceId]
            key, institutionFound = get_institutionKey(institutionOccurrence)

            cluster = clusterDict.setdefault(
                key,
                {
                    "key": key,
                    "institutionKey": key if institutionFound else None,
                    "institutionID": institutionOccurrence.get("institutionID"),
                    "institutionCode": institutionOccurrence.get("institutionCode"),
                    "publishingOrgKey": institutionOccurrence.get("publishingOrgKey"),
                    "data": {},
                },
            )
            treatment_data = cluster["data"].setdefault(
                materialCitationOccurrenceId,
                {
                    "materialCitationOccurrence": materialCitationOccurrence,
                    "institutionOccurrences": [],
                },
            )
            if institutionOccurrence not in treatment_data["institutionOccurrences"]:
                treatment_data["institutionOccurrences"].append(institutionOccurrence)

    # set datasetDocuments with the content of https://api.gbif.org/v1/dataset/{UUID}/document
    for cluster in tqdm(clusterDict.values()):
        datasetKeys = [
            u.get("datasetKey")
            for o in cluster["data"].values()
            for u in o["institutionOccurrences"]
            if "datasetKey" in u
        ]
        cluster["datasetDocuments"] = list(set([datasetKey for datasetKey in datasetKeys]))
    return clusterDict, rows


def distribution(title, data):
    dist = {}
    for value in data:
        dist[value] = dist.get(value, 0) + 1
    print(title)
    print("count:", sum(data))
    print("  min:", min(data))
    print("  max:", max(data))
    if len(dist) < 20:
        print("  distribution:")
        l = list(dist.items())
        l.sort(key=lambda o: o[0])
        for k, v in l:
            print("    * ", k, ":", v)
    else:
        print("  quantiles", [round(q, 1) for q in statistics.quantiles(data, n=10)])


def stat(occurrences, rows, clusterDict):
    references = {}
    combo_org_inst = set()
    for r in rows:
        reference = occurrences[r[0]].get("references", "")
        reference_host = "/".join(reference.split("/")[0:3])
        references[reference_host] = references.get(reference_host, 0) + 1

        institution_occurrence = occurrences[r[1]]
        combo_org_inst.add(
            (
                institution_occurrence.get("institutionKey"),
                institution_occurrence.get("publishingOrgKey"),
            )
        )

    treatmentCountPerInstitution = []
    institutionOccurrenceCountPerTreatment = []
    datasetKeyPerInstitution = []
    for cluster in clusterDict.values():
        treatmentCountPerInstitution.append(len(cluster["data"]))
        datasetKeySet = set()
        for clusterData in cluster["data"].values():
            institutionOccurrenceCountPerTreatment.append(
                len(clusterData["institutionOccurrences"])
            )
            for o in clusterData["institutionOccurrences"]:
                if "datasetKey" in o:
                    datasetKeySet.add(o["datasetKey"])
        datasetKeyPerInstitution.append(len(datasetKeySet))
        if len(datasetKeySet) >= 15:
            print("cluster ", cluster["key"], "has ", len(datasetKeySet), " datasets")

    print(references)
    distribution("Treatment count per institution", treatmentCountPerInstitution)
    distribution("Dataset count per institution", datasetKeyPerInstitution)
    distribution(
        "Institution occurrence count per treatment",
        institutionOccurrenceCountPerTreatment,
    )


def call_matching_algorithm_dataset(dataset_key_dataset):
    dataset_key, dataset = dataset_key_dataset
    for data in dataset["data"].values():
        matchingalgorithm.matching_algorithm_cluster(data)
    return dataset_key, dataset["data"]


print("** matching algorithm")
clusterDict, rows = create_clusters(occurrences)
with Pool(cpu_count()) as pool:
    clusterItems = clusterDict.items()
    for dataset_key, data in tqdm(
        pool.imap_unordered(call_matching_algorithm_dataset, clusterItems),
        total=len(clusterItems),
    ):
        clusterDict[dataset_key]["data"] = data
stat(occurrences, rows, clusterDict)  # optional (display some stats)

print("** remove occurrences")
for datasetkey, dataset in tqdm(clusterDict.items()):
    for data in dataset["data"].values():
        del data["materialCitationOccurrence"]
        data["institutionOccurrences"] = {
            str(o["key"]): o["$score"] for o in data["institutionOccurrences"]
        }

print("** saving")
for datasetkey, dataset in tqdm(clusterDict.items()):
    matcit_per_institutions[datasetkey] = dataset

matcit_per_institutions.close()
