# -*- coding: utf-8 -*-
"""
Script that prints the capabilities of a CSW service for the relevant elements
"""
import requests
import xmltodict
import argparse

from metadata_ingestion._aux import remove_xml_namespaces


def get_cleaned_xml(str_):
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
    operations_data = xml_data['Capabilities']['OperationsMetadata']['Operation']
    get_records_data = [o for o in operations_data if o['@name'] == 'GetRecords'][0]['Parameter']
    print('GetRecords supports the following parameters: \n\n')
    for parameter in get_records_data:
        print('{}: {}'.format(parameter['@name'], parameter['Value']) + '\n')
