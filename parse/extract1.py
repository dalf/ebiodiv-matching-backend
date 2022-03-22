"""
pip install httpx dnspython
"""

import csv
import json
import httpx

import config
from utils import http_get
from diskmap import DiskMap
from tqdm import tqdm


datasets = set()
limit = 300

occurrences = DiskMap(config.occurrences_db, cache=False)

def store_occurrence(occurrence):
    occurrences[str(occurrence["key"])] = occurrence


def new_not_found_occurrence(occurrentId):
    return {
        "key": occurrentId,
        "not_found": True,
    }


def parse_search_response(response, occurrenceIdSet):
    store_count = 0
    for occurrence in response["results"]:
        if occurrence["key"] in occurrenceIdSet:
            store_occurrence(occurrence)
            store_count += 1
    return store_count


def fetch_dataset_size(datasetKey, occurrenceIdSet):
    response = http_get(f"https://api.gbif.org/v1/occurrence/search?datasetKey={datasetKey}&limit=10&offset=0").json()
    parse_search_response(response, occurrenceIdSet)
    return response["count"], response["limit"]


def fetch_dataset_batch(datasetKey, occurrenceIdSet, offset=0):
    global occurences
    found_occurrence_count = 0
    endOfRecords = False
    while not endOfRecords:
        try:
            response = http_get(
                f"https://api.gbif.org/v1/occurrence/search?datasetKey={datasetKey}&limit={limit}&offset={offset}"
            ).json()
        except (httpx.HTTPError, httpx.RequestError) as e:
            print("Error downloading dataset", datasetKey, e)
            break
        except json.JSONDecodeError as e:
            print("Error parsing dataset", datasetKey, e)
            break
        else:
            found_occurrence_count += parse_search_response(response, occurrenceIdSet)
            # next
            count = response["count"]
            endOfRecords = response["endOfRecords"]
            offset += response["limit"]  # offset can't be heigher than 100001
            print(
                "       ",
                found_occurrence_count,
                "/",
                len(occurrenceIdSet),
                ", batch: ",
                offset,
                "/",
                count,
                end="\r",
            )
            if len(occurrenceIdSet) - found_occurrence_count == 0:
                # all requested occurrenes have been found
                # we can stop here
                break
    else:
        return True
    return False


def fetch_dataset_occurrences(occurrenceIdSet):
    for i, occurrenceKey in enumerate(occurrenceIdSet):
        if occurrenceKey not in occurrences:
            try:
                response = http_get(f"https://api.gbif.org/v1/occurrence/{occurrenceKey}").json()
            except (httpx.HTTPError, httpx.RequestError) as e:
                print("Can't find occurrenceId ", occurrenceKey, e)
                response = new_not_found_occurrence(occurrenceKey)
            store_occurrence(response)
            print("       ", i, "/", len(occurrenceIdSet), end="\r")
    print("       ", len(occurrenceIdSet), "/", len(occurrenceIdSet), end="\r")


def fetch_dataset(datasetKey, occurrenceIdSet, dataset_indice, dataset_count):
    dataset_size, already_done_count = fetch_dataset_size(datasetKey, occurrenceIdSet)
    if ((dataset_size - already_done_count) / limit) < (len(occurrenceIdSet) - already_done_count) and len(
        occurrenceIdSet
    ) > 1:
        print(
            dataset_indice,
            dataset_count,
            "dataset",
            datasetKey,
            "🏭 batch,       ",
            len(occurrenceIdSet),
            " occurrence(s), dataset size:",
            dataset_size,
        )
        fetch_dataset_batch(datasetKey, occurrenceIdSet, offset=already_done_count)
        fetch_dataset_occurrences(occurrenceIdSet)
    else:
        print(
            dataset_indice,
            dataset_count,
            "dataset",
            datasetKey,
            "🔨 occurrences, ",
            len(occurrenceIdSet),
            " occurrence(s), dataset size:",
            dataset_size,
        )
        return fetch_dataset_occurrences(occurrenceIdSet)


def download_dataset(datasetKey, occurrenceIdSet, dataset_indice, dataset_count):
    global datasets
    if datasetKey in datasets:
        return False
    # download
    fetch_dataset(datasetKey, occurrenceIdSet, dataset_indice, dataset_count)
    #
    datasets.add(datasetKey)
    return True


def download_dataset_list():
    global datasets

    dataset_occurrences = dict()
    occurrence_pairs = set()

    with open(config.plazi_records) as csvfile:
        occurence_reader = csv.reader(csvfile, delimiter=";", quotechar="|")
        next(occurence_reader, None)
        for row in tqdm(occurence_reader):
            for occurrenceId, datasetKey in [
                (row[0], row[2]),
                (row[1], row[3]),
            ]:
                if occurrenceId not in occurrences:
                    dataset_occurrences.setdefault(datasetKey, set()).add(occurrenceId)
            t = (int(row[0]), int(row[1]))
            occurrence_pairs.add(t)

    dataset_occurrences_list = list(dataset_occurrences.items())
    dataset_occurrences_list.sort(key=lambda i: len(i[1]))
    for dataset_indice, info in enumerate(dataset_occurrences_list):
        download_dataset(info[0], info[1], dataset_indice, len(dataset_occurrences_list))


if __name__ == '__main__':
    download_dataset_list()
    occurrences.close()
