import requests
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urljoin
import logging
import sys
import json
from pathlib import Path


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


REQUEST_SIZE = 25
HOST = 'https://horreum:ehAqJvXAnV2A1j0BW2i5Vla1@es-access.prime-intra.net:8777/'
relative_url = (
    'music-direct-access/'
    'cisioninsights_prime_research_one_index_v20_2022_05,cisioninsights_prime_research_one_index_v20_2022_06/'
    '_search'
)
url = urljoin(HOST, relative_url)
query = json.load(open("query.json"))

i = 0
retrieved_records = 0

with requests.Session() as session:
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[404, 504]
    )
    session.mount(HOST, HTTPAdapter(max_retries=retries))

    while True:
        offset = i * REQUEST_SIZE
        query['size'] = str(REQUEST_SIZE)
        query['from'] = str(offset)
        logger.info(f"Querying ElasticSearch for data from {offset}")
        response = session.get(url, json=query)
        response.raise_for_status()
        data = response.json()

        total_records = data['hits']['total']['value']
        retrieved_records += len(data['hits']['hits'])
        logger.info(f"{retrieved_records} of {total_records} total records retrieved.")

        filename = f"{offset:04d}.json"
        logger.info(f"Writing response to file: {filename}")
        with open(Path("../data/raw", filename), 'w') as f:
            json.dump(data, f)

        if retrieved_records >= total_records:
            break
        else:
            i += 1

logger.info("Done")