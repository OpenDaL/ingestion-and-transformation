# -*- coding: utf-8 -*-
"""
This script creates a new index on the elasticsearch instance, and uploads all
data to that instance

Copyright (C) 2021  Tom Brouwer

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import json
import os
import logging
from logging import handlers
from pathlib import Path
import getpass
import argparse
from typing import Union, Any

import requests

INDEX_NAME = 'resource_metadata'
SEND_PER = 500
HEADERS = {'content-type': 'application/json'}

rsession = requests.session()


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """
    Validate if an input argument is a existing folder
    """
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_json_file(parser: argparse.ArgumentParser, fileloc: str) -> Path:
    """
    Validate if input argument is an existing JSON file
    """
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    elif not path.name.endswith('.json'):
        parser.error('The file {} is not a JSON file'.format(fileloc))
    else:
        return path


def is_valid_file_location(
        parser: argparse.ArgumentParser, fileloc: str,
        ) -> Path:
    """
    Validate if an input argument is a valid location for a file
    """
    path = Path(fileloc)
    dir = path.parent
    if not dir.is_dir():
        parser.error(
            'The file {} is not in an existing directory'.format(fileloc)
        )
    else:
        return path


def load_mapping(mloc: Union[Path, str]) -> Any:
    """Loads the mapping json"""
    with open(mloc, 'r', encoding='utf8') as jsonfile:
        return json.load(jsonfile)


def create_es_format(entry: dict, root_keys: set):
    """
    Converts the entry to the format for ES, according to the mapping.
    Operates on the input dict (In-place edits)
    """
    key_names = list(entry.keys())
    entry['notIndexed_'] = {}

    for key in key_names:
        if key not in root_keys:
            entry['notIndexed_'][key] = entry.pop(key)


if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Upload processed data to ElasticSearch"
    )
    aparser.add_argument(
        "folder",
        help="The folder containing the (processed) data to upload to ES",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "es_host",
        help="IP Address/Domain of ElasticSearch instance (e.g. 127.0.0.1)",
        type=str
    )
    aparser.add_argument(
        "--mapping",
        help="Location of the mapping.json",
        type=lambda x: is_valid_json_file(aparser, x),
        required=True,
    )
    aparser.add_argument(
        "--log",
        help="Location of the log (By default, stored in current directory)",
        type=lambda x: is_valid_file_location(aparser, x),
        default=Path('es_upload.log').absolute(),
    )

    # Get arguments
    args = aparser.parse_args()
    data_folder = args.folder
    es_ip = args.es_host
    mapping_loc = args.mapping
    log_loc = args.log

    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = handlers.RotatingFileHandler(log_loc, maxBytes=1048576,
                                           backupCount=5)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Set password if given
    es_pass = getpass.getpass(
        "Password for the 'data_upload' ES user (Empty if None): "
    )
    if es_pass != '':
        rsession.auth = ('data_upload', es_pass)

    # Load ES mapping
    es_mapping = load_mapping(mapping_loc)

    # Create ES index:
    es_url = 'http://{}:9200/'.format(es_ip)
    es_address = ''.join([es_url, INDEX_NAME])

    logger.info('Starting data upload to {} from folder {}'.format(
        es_address,
        data_folder
    ))
    response = rsession.put(
        es_address,
        data=json.dumps(es_mapping, ensure_ascii=False).encode('utf8'),
        headers=HEADERS
    )
    response.raise_for_status()

    # Determine keys that will be indexed:
    indexed_keys = set([
        k for k in es_mapping['mappings']['properties'] if not k.endswith('_')
    ])
    bulk_address = es_address + '/_bulk'

    # Get a list of file names
    processed_fns = [
        fn for fn in os.listdir(data_folder)
        if fn.endswith('.jl') or fn.endswith('.jsonl')
    ]

    # Load from json-lines file, and push data to ES, log any fatal errors:
    try:
        between_count = 0
        count = 0
        queue = []
        for filename in processed_fns:
            file_count = 0
            logger.info('Processing file {}'.format(filename))
            file_loc = os.path.join(data_folder, filename)
            with open(file_loc, 'r', encoding='utf8') as jsonlinesfile:
                for line in jsonlinesfile:
                    file_count += 1
                    between_count += 1
                    count += 1
                    entry = json.loads(line)
                    create_es_format(entry, indexed_keys)
                    id_data = {'index': {'_id': entry['id']}}
                    queue.append(json.dumps(id_data, ensure_ascii=False) + '\n'
                                 + json.dumps(entry, ensure_ascii=False))
                    if len(queue) == SEND_PER:
                        payload = '\n'.join(queue) + '\n'
                        queue = []
                        response = rsession.post(bulk_address,
                                                 data=payload.encode('utf-8'),
                                                 headers=HEADERS)
                        response.raise_for_status()
                        rdata = response.json()
                        assert len(rdata['items']) == 500,\
                            "Not all items were processed!"
                        for item in rdata['items']:
                            status = item['index']['status']
                            if status == 200:
                                logger.warning(
                                    'Duplicate ID: {}'.format(
                                        item['index']['_id']
                                    )
                                )
                            elif status != 201:
                                logger.warning(
                                    'The Following item failed to push: ' +
                                    json.dumps(
                                        item, ensure_ascii=False, indent=4
                                    )
                                )
                        if between_count >= 10000:
                            print('send {} items'.format(count))
                            between_count = 0
                logger.info('Uploading {} entries of file {}'.format(
                    file_count,
                    filename
                ))
        else:
            if len(queue) > 0:
                payload = '\n'.join(queue) + '\n'
                queue = []
                response = rsession.post(bulk_address,
                                         data=payload.encode('utf-8'),
                                         headers=HEADERS)
                response.raise_for_status()
                rdata = response.json()
                for item in rdata['items']:
                    status = item['index']['status']
                    if status == 200:
                        logger.warning(
                            'Duplicate ID: {}'.format(item['index']['_id'])
                        )
                    elif status != 201:
                        logger.error(
                            'The Following item failed to push: {}'.format(
                                json.dumps(item, ensure_ascii=False, indent=4)
                            ))
                if between_count > 10000:
                    print('send {} items'.format(count))
                    between_count = 0
        # Set the index to read_only
        rsession.put(
            es_address + '/_settings',
            data=json.dumps({'index.blocks.read_only': True}).encode('utf8'),
            headers=HEADERS
        )
        # Force merge to optimize performance
        rsession.post(
            es_address + '/_forcemerge',
            headers=HEADERS
        )

        # Refresh search 1 time (required if refresh_interval == -1):
        rsession.post(
            es_address + '/_refresh',
            headers=HEADERS
        )
    except Exception:
        logger.exception('The following fatal exception occured:')
        logger.info(
            'Following data was returned last: {}'.format(response.text)
        )
        raise
