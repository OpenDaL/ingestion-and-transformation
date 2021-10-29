# -*- coding: utf-8 -*-
"""
Script that structures, translates and post_processes harvested data stored
in json-lines files

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
import os
import re
import json
from pathlib import Path
import time
import argparse
import logging
from logging import handlers

from metadata_ingestion import _loadcfg, structurers

MEMORIZE = 2000

sources = _loadcfg.sources()

filename_regex = re.compile(
    r'(.*)_\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z?\.(jl|jsonl)$'
)


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """Check if the provided directory is valid"""
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_file(parser: argparse.ArgumentParser, fileloc: str) -> Path:
    """Check if the given file loc is valid"""
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    else:
        return path


def is_valid_output_file(
        parser: argparse.ArgumentParser, fileloc: str
        ) -> Path:
    """Check if the given output location is in an existing directory"""
    path = Path(fileloc)
    if not path.absolute().parent.is_dir():
        parser.error('Output file directory does not exist')
    else:
        return path


def process_data(
        in_data: list[dict], pinfo: dict, out_loc: Path, method: str,
        logger: logging.Logger, structurer: structurers.Structurer
        ):
    """
    Process the list of data

    Args:
        in_data:
            The list of data to process
        pinfo:
            Dictionary to track the progress
        out_loc:
            The location to store the processed data
        method:
            The write method (as provided to the 'open' function) for the
            output
        logger:
            The logger to log the progress on
        structurer:
            The structurer object to use to structure the data
    """
    global last_log_time

    out_data = []
    for i, payload in enumerate(in_data):
        pinfo['total_processed'] += 1
        line_nr = pinfo['total_processed']
        if time.time() - last_log_time > 10:
            logger.info('Processed {} lines'.format(line_nr))
            last_log_time = time.time()
        try:
            metadata = structurer.structure(payload)

        except Exception:
            logger.exception(
                'An exception occured at line {}, with payload:\n{}'.format(
                    line_nr,
                    payload
                )
            )
            raise

        out_data.append(
            {
                'meta': metadata.meta,
                'structured': metadata.structured,
                'is_filtered': metadata.is_filtered
            }
        )
        pinfo['result_count'] += 1

    in_data.clear()

    with open(out_loc, method, encoding='utf8') as jsonl_file:
        for dat in out_data:
            jsonl_file.write(
                json.dumps(dat, ensure_ascii=False) + '\n'
            )


def process_data_file(input_loc: Path, output_dir: Path):
    """
    Processes a single file of data, halt on errors
    """
    filename = os.path.split(input_loc)[-1]
    fn_id = filename_regex.match(filename).group(1)
    out_loc = os.path.join(output_dir, filename)

    source_data = [s for s in sources if s['id'] == fn_id][0]
    structurer_kwargs = source_data.get('structurer_kwargs', {})
    structurer_name = source_data['structurer']

    structurer = getattr(structurers, structurer_name)(
        fn_id, **structurer_kwargs
    )

    with open(input_loc, 'r', encoding='utf8') as jsonlinesfile:
        in_data = []
        method = 'w'
        process_info = {'total_processed': 0, 'result_count': 0}
        for line in jsonlinesfile:
            in_data.append(json.loads(line))
            nr_collected = len(in_data)
            if nr_collected == MEMORIZE:
                process_data(
                    in_data, process_info, out_loc, method, logger, structurer
                )
                method = 'a'
        else:
            nr_collected = len(in_data)
            process_data(
                    in_data, process_info, out_loc, method, logger, structurer
            )
        logger.info(
            (
                'finished processing, processed {} lines'
                ', yielding {} results'
            ).format(
                process_info['total_processed'],
                process_info['result_count']
            )
        )


if __name__ == '__main__':
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Process data from a single portal into the OpenDaL format"
    )
    aparser.add_argument(
        "in_file",
        help="The input (harvested) data file",
        type=lambda x: is_valid_file(aparser, x)
    )
    aparser.add_argument(
        "out_folder",
        help="The folder to save the (processed) output data",
        type=lambda x: is_valid_folder(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    in_loc = args.in_file
    out_dir = args.out_folder

    log_loc = os.path.join(out_dir, 'processing.log')
    logger = logging.getLogger()
    handler = handlers.RotatingFileHandler(
        log_loc, maxBytes=1048576, backupCount=4
    )
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info('Start processing')

    # Set start time, to be able to print progress every 10 seconds
    last_log_time = time.time()

    process_data_file(in_loc, out_dir)
