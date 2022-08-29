from hashlib import new
import json
from glob import glob
import re
import pandas as pd
import logging
import sys
from pathlib import Path

INPUT_DIR = "../data/raw/"
OUTPUT_DIR = "../data/interim/"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class UnidentifiedMatcherType(Exception):
    pass


def paragraph_matcher(paragraphs):
    def matcher(key):
        ix = int(key.replace('paragraph_', ''))
        return paragraphs[ix]
    return matcher


def title_matcher(title):
    def matcher(key):
        return title
    return matcher


def create_matcher(paragraphs, title):
    def _matcher(key):
        if key == 'title':
            return title_matcher(title)(key)
        elif key.startswith('paragraph'):
            return paragraph_matcher(paragraphs)(key)
        else:
            raise UnidentifiedMatcherType(key)
    return _matcher


def parse_document(document):
    records = []
    doc_id = document['_id']
    text = document['_source']['text']
    title = document['_source']['title']
    language = document['_source']['language']
    paragraphs = [p.strip() for p in re.split('\n|\r', text) if p]
    coded_items = document['_source']['codedItems']

    element_matcher = create_matcher(paragraphs, title)

    for coded_item in coded_items:
        matched_in = coded_item['matchedIn']
        obj = coded_item['object']
        try:
            records.append({
                'doc_id': doc_id,
                'language': language,
                'matched_in': matched_in,
                'text': element_matcher(matched_in),
                'item_type': coded_item['type'],
                'label': obj.get('label'),
                'display_label': obj.get('displayLabel'),
                'label_hierarchy': obj.get('labelHierarchy'),
                'mspell': obj.get('mspell'),
                'sentiment': coded_item['sentiment'],
            })
        except UnidentifiedMatcherType:
            pass
    
    return records


if __name__ == '__main__':
    logger.info(f"Starting parse")

    files = glob(str(Path(INPUT_DIR, '*.json')))
    logger.info(f"{len(files)} total files.")

    for file in files:
        logger.info(f"Parsing {file}")
        records = []
        raw = json.load(open(file))
        documents = raw['hits']['hits']
        logger.info(f"{len(documents)} documents")

        for doc in documents:
            try:
                records += parse_document(doc)
            except (KeyError, IndexError):
                logger.error(f"Unable to parse {doc['_id']}")

        records_df = pd.DataFrame(records)

        new_file = Path(
            OUTPUT_DIR, 
            Path(file).name.replace('.json', '.parquet')
        )
        logger.info(f"Writing to {new_file}")
        records_df.to_parquet(new_file, index=False)

    logger.info(f"Done")