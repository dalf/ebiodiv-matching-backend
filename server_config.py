import configparser

config = configparser.ConfigParser()
config.read_string("""
[server]
base_url=/

[database]
uri=sqlite:///matching.db

[api]
fields=key, scientificName, verbatimLabel, collectionCode, collectionKey, continent, country, countryCode, stateProvince, county, municipality, locality, depth

[datasource]
url=https://tb.plazi.org/GgServer/gbifOccLinkData/
timeout=180
""")
config.read('server.ini')

# on production: https://candy.text-analytics.ch/ebiodiv/matching/api
BASE_URL = config['server']['base_url']
DATABASE_URI = config['database']['uri']
FIELDS = [f.strip() for f in config['api']['fields'].split(',')]
DATASOURCE_URL = config['datasource']['url']
DATASOURCE_TIMEOUT = int(config['datasource']['timeout'])

if __name__ == '__main__':
    print(BASE_URL)
    print(DATABASE_URI)
    print(FIELDS)
    print(DATASOURCE_URL)
