# -*- coding: utf-8 -*-
"""
This script counts the lines of all harvested/processed files in a directory,
and registers the file-size of the files
"""
from pathlib import Path
import json
import re
import argparse
from typing import Union

filename_regex = re.compile(
    r'(.*)_\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z?\.(jl|jsonl)$'
)


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """
    Check if the provided dirloc is valid
    """
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def count_lines(input_loc: Union[Path, str]) -> int:
    """
    Count the lines in the given file
    """
    count = 0
    with open(input_loc, 'r', encoding='utf8') as linesfile:
        for line in linesfile:
            count += 1

    return count


if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description=(
            "Create a statistics file (stats.json) for a directory"
            " with scraped or processed data"
        )
    )
    aparser.add_argument(
        "folder",
        help="The folder for which the stats file should be generated",
        type=lambda x: is_valid_folder(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    stats_folder = args.folder

    # Create a list of all filenames and their id's
    file_data = []
    ids = set()
    for p in Path(stats_folder).iterdir():
        match = filename_regex.match(p.name)
        if match is not None:
            id_ = match.group(1)
            if id_ in ids:
                raise ValueError(
                    'The following id appears multiple times: {}'.format(
                        id_
                        )
                    )
            ids.add(id_)
            file_data.append((p, id_))

    # Determine file size and line count for each file
    files_stats = {}
    for p, id_ in file_data:
        print('Processing {}'.format(id_))
        files_stats[id_] = {
            'count': count_lines(p),
            'size': p.stat().st_size
        }

    # Write these to a jsonfile in the directory
    with open(Path(stats_folder, 'stats.json'), 'w', encoding='utf8') as\
            jsonfile:
        json.dump(files_stats, jsonfile, ensure_ascii=False, indent=4)

    print('\nStats saved to stats.json')
