# -*- coding: utf-8 -*-
"""
Script to delete an existing ES index

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
