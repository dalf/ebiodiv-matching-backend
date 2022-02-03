import csv
from tqdm import tqdm
import xmltodict

import config
from diskmap import DiskMap
from utils import http_get


occurrences = DiskMap(config.occurrences_db, readonly=True)
datasetmetadata = DiskMap(config.datasetmetadata_db)
institutions = DiskMap(config.institutions_db)

institutions_per_code = dict()
institutions_per_id = dict()
for institution in institutions.values():
    if "code" in institution:
        institutions_per_code[institution["code"]] = institution
    for identifier in institution.get("identifiers", []):
        institutions_per_id[identifier["identifier"]] = institution

collections = dict()


rows = []
with open(config.plazi_records) as csvfile:
    occurence_reader = csv.reader(csvfile, delimiter=";", quotechar="|")
    count = 0
    for row in tqdm(occurence_reader):
        count += 1
        if count > 1:
            rows.append([row[0], row[1], row[2], row[3]])


def fetch(occurrences, rows, institutions, datasetmetadata):
    print("len(institutions)=", len(institutions))
    institutionKeySet = set()
    datasetKeySet = set()
    for row in tqdm(rows):
        institutionKeySet.add(occurrences[row[1]].get("institutionKey"))
        datasetKeySet.add(occurrences[row[1]].get("datasetKey"))

    for institutionKey in tqdm(institutionKeySet):
        if isinstance(institutionKey, str) and institutionKey not in institutions:
            try:
                institutions[institutionKey] = http_get(
                    "https://api.gbif.org/v1/grscicoll/institution/" + institutionKey
                ).json()
            except:
                institutions[institutionKey] = {"not_found": True}

    for datasetKey in tqdm(datasetKeySet):
        if isinstance(datasetKey, str) and datasetKey not in datasetmetadata:
            try:
                content = http_get(f"https://api.gbif.org/v1/dataset/{datasetKey}/document").text
            except:
                datasetmetadata[datasetKey] = {"not_found": True}
            else:
                datasetmetadata[datasetKey] = xmltodict.parse(content)
    return institutions, datasetmetadata


def get_institutionKey(occurrence):
    if "institutionKey" in occurrence:
        return occurrence["institutionKey"], True

    if "institutionCode" in occurrence:
        institutionCode = occurrence["institutionCode"]
        if institutionCode in institutions_per_code and "key" in institutions_per_code[institutionCode]:
            return institutions_per_code[institutionCode]["key"], True

    if "institutionID" in occurrence:
        institutionID = occurrence["institutionID"]
        if institutionID in institutions_per_id and "key" in institutions_per_id[institutionID]:
            return institutions_per_id[institutionID]["key"], True

    return None, False


def search_for_organization(name, emails):
    response = http_get(f"https://api.gbif.org/v1/organization?q={name}").json()["results"]
    for organization in response:
        org_emails = set()
        for contact in organization.get("contacts", []):
            for email in contact.get("email"):
                org_emails.add(email)
        if organization.get("title") == name and len(emails.intersection(org_emails)) > 0:
            return organization


def set_organization_for_dataset(metadata):
    if "$organizationKey" in metadata:
        return

    emails = set()
    contact = metadata.get("eml:eml", {}).get("dataset", {}).get("contact", [])
    if isinstance(contact, dict) and "electronicMailAddress" in contact:
        emails.add(contact["electronicMailAddress"])
    elif isinstance(contact, list):
        for one_contact in contact:
            if isinstance(one_contact, list):
                if "electronicMailAddress" in one_contact:
                    emails.add(one_contact["electronicMailAddress"])
            elif isinstance(one_contact, dict) and "electronicMailAddress" in one_contact:
                emails.add(one_contact["electronicMailAddress"])
    organizations = []
    creator_list = metadata.get("eml:eml", {}).get("dataset", {}).get("creator", [])
    if isinstance(creator_list, dict):
        creator_list = [creator_list]
    for creator in creator_list:
        if "organizationName" in creator and creator["organizationName"] not in organizations:
            organizations.append(creator["organizationName"])
    if len(organizations) != 1:
        return
    org = search_for_organization(organizations[0], emails)
    if org:
        metadata["$organizationKey"] = org["key"]
        institutions[org["key"]] = org


institutions, datasetmetadata = fetch(occurrences, rows, institutions, datasetmetadata)
for institution in institutions.values():
    if "code" in institution:
        institutions_per_code[institution["code"]] = institution
    for identifier in institution.get("identifiers", []):
        institutions_per_id[identifier["identifier"]] = institution


def search_one(url):
    result = http_get(url).json()
    if len(result["results"]) == 1:
        return result["results"][0]
    return None    


def search_one_institution(url):
    result = search_one(url)
    if result:
        institutions[result["key"]] = result
        return result
    return None


def fetch_one_collection(occurrence):
    if 'collectionKey' in occurrence:
        return http_get('https://api.gbif.org/v1/grscicoll/collection/' + occurrence['collectionKey'])
    if 'collectionCode' in occurrence:
        return search_one('https://api.gbif.org/v1/grscicoll/collection?code=' + occurrence['collectionCode'])
    if 'collectionID' in occurrence:
        return search_one('https://api.gbif.org/v1/grscicoll/collection?identifier=' + occurrence['collectionID'])
    return None


for row in tqdm(rows):
    occurrence = occurrences[row[1]]
    institutionKey, institutionFound = get_institutionKey(occurrence)
    if institutionFound:
        continue

    if "institutionCode" in occurrence and occurrence["institutionCode"] not in institutions_per_code:
        institution = search_one_institution(
            "https://api.gbif.org/v1/grscicoll/institution?code=" + occurrence["institutionCode"]
        )
        institutions_per_code[occurrence["institutionCode"]] = institution or {"not_found": True}

    if "institutionID" in occurrence and occurrence["institutionID"] not in institutions_per_id:
        institution = search_one_institution(
            "https://api.gbif.org/v1/grscicoll/institution?identifier=" + occurrence["institutionID"]
        )
        institutions_per_id[occurrence["institutionID"]] = institution or {"not_found": True}

    collection = fetch_one_collection(occurrence)
    if collection:
        collections[collection["key"]] = collection
        if 'institutionKey' in collection:
            institution = http_get('https://api.gbif.org/v1/grscicoll/institution/' + collection['institutionKey']).json()
            institutions[institution['key']] = institution

    if "datasetKey" in occurrence:
        set_organization_for_dataset(datasetmetadata[occurrence["datasetKey"]])


institutions.close()
datasetmetadata.close()

with DiskMap.open(config.collections_db) as m:
    m.store(collections)
