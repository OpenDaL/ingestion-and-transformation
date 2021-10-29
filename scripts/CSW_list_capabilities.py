# -*- coding: utf-8 -*-
"""
Script that prints the capabilities of a CSW service for the relevant elements

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
import xmltodict
import argparse
from typing import Any

from metadata_ingestion._common import remove_xml_namespaces


def get_cleaned_xml(str_) -> Any:
    """
    Convert XML string to cleaned data in a dict
    """
    parsed_xml = xmltodict.parse(str_)
    return remove_xml_namespaces(parsed_xml)


if __name__ == "__main__":
    aparser = argparse.ArgumentParser(
        description="Sends a listCapabilities request to a CSW endpoint"
    )
    aparser.add_argument(
        "endpoint",
        help="Base URL CSW endpoint (without URL parameters)",
        type=str
    )
    args = aparser.parse_args()
    BASE_URL = args.endpoint

    getcp_url = BASE_URL + '?service=CSW&request=GetCapabilities'

    response = requests.get(getcp_url)
    xml_data = get_cleaned_xml(response.text)
    print('CSW Version: {}'.format(xml_data['Capabilities']['@version']))
    operations_data =\
        xml_data['Capabilities']['OperationsMetadata']['Operation']
    get_records_data = [
        o for o in operations_data if o['@name'] == 'GetRecords'
    ][0]['Parameter']
    print('GetRecords supports the following parameters: \n\n')
    for parameter in get_records_data:
        print('{}: {}'.format(parameter['@name'], parameter['Value']) + '\n')
