import configparser

config = configparser.ConfigParser()
config.read_string("""
[server]
base_url=/

[datasource]
url=https://tb.plazi.org/GgServer/gbifOccLinkData/
timeout=180
""")
config.read('server.ini')

# on production: https://candy.text-analytics.ch/ebiodiv/matching/proxy
BASE_URL = config['server']['base_url']
DATASOURCE_URL = config['datasource']['url']
DATASOURCE_TIMEOUT = int(config['datasource']['timeout'])

if __name__ == '__main__':
    print(BASE_URL)
    print(DATASOURCE_URL)
    print(DATASOURCE_TIMEOUT)
