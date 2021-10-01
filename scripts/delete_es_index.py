# -*- coding: utf-8 -*-
"""
Script to delete an existing ES index
"""
import requests
import json
import argparse

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Delete an ES index that was previously set to read-only"
    )
    aparser.add_argument(
        "host",
        help="IP/Host of the ES instance (e.g. 127.0.0.1)",
        type=str
    )
    aparser.add_argument(
        "index",
        help="ID of the ES index to delete",
        type=str
    )
    args = aparser.parse_args()
    es_ip = args.host
    INDEX = args.index

    BASE_ADDRESS = 'http://{}:9200/'.format(es_ip)
    DELETE_ADRESS = ''.join([BASE_ADDRESS, INDEX])

    response = requests.put(
        DELETE_ADRESS + '/_settings',
        data=json.dumps({'index.blocks.read_only': False}).encode('utf8'),
        headers={'Content-Type': 'application/json'}
    )

    response = requests.delete(DELETE_ADRESS)
    print(response.text)
    response.raise_for_status()
