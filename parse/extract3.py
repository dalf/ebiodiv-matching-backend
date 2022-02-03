from tqdm import tqdm

import config
from diskmap import DiskMap
from utils import http_get


occurrences = DiskMap(config.occurrences_db)
organization_set = set()

for occurrence in tqdm(occurrences.values()):
    for k in ["hostingOrganizationKey", "publishingOrgKey"]:
        orgKey = occurrence.get(k)
        if orgKey:
            organization_set.add(orgKey)

print(len(organization_set), " organization(s)")
organization = {}
for orgKey in tqdm(sorted(list(organization_set))):
    organization[orgKey] = http_get("https://api.gbif.org/v1/organization/" + orgKey).json()

with DiskMap.open(config.organizations_db) as m:
    m.store(organization)
