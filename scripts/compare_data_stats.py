# -*- coding: utf-8 -*-
"""
This script compares two statistics files, and warns when significant changes
are detected.
"""
from pathlib import Path
import json
import argparse
from typing import Union


def is_valid_json_file(parser: argparse.ArgumentParser, fileloc: str):
    """
    Checks if the given fileloc is a valid JSON file
    """
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    elif not path.name.endswith('.json'):
        parser.error('The file {} is not a JSON file'.format(fileloc))
    else:
        return path


def load_json(file_loc: Union[Path, str]):
    with open(file_loc, 'r', encoding='utf8') as jsonfile:
        return json.load(jsonfile)


if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Compare newly created statistics files to previous files"
    )
    aparser.add_argument(
        "file1",
        help="File with statistics of previous iteration",
        type=lambda x: is_valid_json_file(aparser, x)
    )
    aparser.add_argument(
        "file2",
        help="File with statistics of new iteration",
        type=lambda x: is_valid_json_file(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    prev_file = args.file1
    new_file = args.file2

    # Load both files
    prev_data = load_json(prev_file)
    new_data = load_json(new_file)

    # Iterate through the new data, and check against the old
    for id_, stats in new_data.items():
        message = ''
        if id_ in prev_data:
            old_count = prev_data[id_]['count']
            new_count = new_data[id_]['count']
            old_size = prev_data[id_]['size']
            new_size = new_data[id_]['size']

            # Check counts, distinguish between small and large portals
            if new_count < 10:
                if new_count < (0.9 * old_count):
                    message += '* Count reduced by > 10%\n'
                if new_count > (2 * old_count):
                    message += '* Count more than doubled\n'
            else:
                if new_count < (0.96 * old_count):
                    message += '* Count reduced by > 5%\n'
                if new_count > (1.3 * old_count):
                    message += '* Count increased by more than 30%\n'

            # Check file size, no distguishing between small and large portals
            if new_size < 1048576:  # One megabyte
                if new_size < (0.90 * old_size):
                    message += '* Size reduced by > 10%\n'
                if new_size > (2 * old_size):
                    message += '* Size more than doubled\n'
            else:
                if new_size < (0.98 * old_size):
                    message += '* Size reduced by > 2%\n'
                if new_size > (1.2 * old_size):
                    message += '* Size increased by more than 20%\n'

            if message != '':
                # Log the old properties and the new ones
                message += f'= Previous count: {old_count}\n'
                message += f'  New count:      {new_count}\n'
                message += f'= Previous Size: {old_size}\n'
                message += f'  New Size:      {new_size}\n'
        else:
            message += '** NEW FILE **\n'

        if message != '':
            message = id_ + '\n' + message
            print(message)

    # Check if any portals are in the old data but not in the new
    prev_ids = set(prev_data.keys())
    new_ids = set(new_data.keys())
    missing_ids = prev_ids - new_ids
    for id_ in missing_ids:
        print('{}\n** FILE MISSING **'.format(id_))
