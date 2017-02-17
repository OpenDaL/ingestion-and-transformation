# -*- coding: utf-8 -*-
"""
Script that structures, translates and post_processes harvested data stored
in json-lines files
"""
import os
import re
import json
from pathlib import Path
import time
import argparse

from metadata_ingestion import _loadcfg, structure, translate, post_process

MEMORIZE = 2000

sources = _loadcfg.sources()

filename_regex = re.compile(
    r'(.*)_\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z?\.(jl|jsonl)$'
)


def is_valid_folder(parser, dirloc):
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_file(parser, fileloc):
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    else:
        return path


def is_valid_output_file(parser, fileloc):
    path = Path(fileloc)
    if not path.absolute().parent.is_dir():
        parser.error('Output file directory does not exist')
    else:
        return path


def process_data(in_data, pinfo, out_loc, method, logger, dformat,
                 structurer_kwargs, dplatform_id, rejects_file=None):
    """
    Process the list of data
    """
    global last_log_time

    out_data = []
    reject_data = []
    for i, payload in enumerate(in_data):
        pinfo['total_processed'] += 1
        line_nr = pinfo['total_processed']
        if time.time() - last_log_time > 10:
            logger.info('Processed {} lines'.format(line_nr))
            last_log_time = time.time()
        try:
            # Structuring
            structured_entry = structure.single_entry(
                payload,
                dformat,
                **structurer_kwargs
            )

            if structured_entry is None:
                # Allows for filtering items
                reject_data.append([payload, None])
                continue

            # Translation
            translated_entry =\
                translate.single_entry(structured_entry,
                                       dplatform_id)

            # Post Processing
            if enable_post_filter and\
                    post_process.is_filtered(translated_entry):
                reject_data.append([payload, translated_entry])
                continue
            else:
                post_process.optimize(translated_entry)
                post_process.score(translated_entry)

        except Exception:
            logger.exception(
                'An exception occured at line {}, with payload:\n{}'.format(
                    line_nr,
                    payload
                )
            )
            raise

        out_data.append(translated_entry)
        pinfo['result_count'] += 1

    in_data.clear()

    with open(out_loc, method, encoding='utf8') as jsonl_file:
        for dat in out_data:
            jsonl_file.write(
                json.dumps(dat, ensure_ascii=False) + '\n'
            )

    if rejects_loc:
        with open(rejects_loc, method, encoding='utf8') as jsonl_file:
            for dat in reject_data:
                jsonl_file.write(
                    json.dumps(dat, ensure_ascii=False) + '\n'
                )


def process_data_file(input_loc, output_dir):
    """
    Processes a single file of data, halts on errors
    """
    # Setup logging, specifically for this process
    import logging
    from logging import handlers

    LOG_LOC = os.path.join(output_dir, 'processing.log')
    logger = logging.getLogger()
    handler = handlers.RotatingFileHandler(LOG_LOC,
                                           maxBytes=1048576,
                                           backupCount=4)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info('Start processing')

    filename = os.path.split(input_loc)[-1]
    fn_id = filename_regex.match(filename).group(1)
    out_loc = os.path.join(output_dir, filename)

    source_data = [s for s in sources if s['id'] == fn_id][0]
    structurer_kwargs = source_data.get('structurer_kwargs', {})

    dformat = source_data['data_format']
    dplatform_id = source_data['id']

    with open(input_loc, 'r', encoding='utf8') as jsonlinesfile:
        in_data = []
        method = 'w'
        process_info = {'total_processed': 0, 'result_count': 0}
        for line in jsonlinesfile:
            in_data.append(json.loads(line))
            nr_collected = len(in_data)
            if nr_collected == MEMORIZE:
                process_data(in_data, process_info, out_loc, method, logger,
                             dformat, structurer_kwargs, dplatform_id)
                method = 'a'
        else:
            nr_collected = len(in_data)
            process_data(in_data, process_info, out_loc, method, logger,
                         dformat, structurer_kwargs, dplatform_id)
        logger.info('finished processing, processed {} lines, yielding {} results'.format(
            process_info['total_processed'],
            process_info['result_count']
        ))


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
    aparser.add_argument(
        "--no-post-filter",
        help=(
            "Disable the post_process.is_filtered function, to investigate"
            " what's translated"
        ),
        action="store_false",
        dest='post_filter'
    )

    aparser.add_argument(
        "--rejects-file",
        help=(
            "Save any data filtered in structuring or post processing to this"
            " location (JSON lines file)"
        ),
        dest='rejects_loc',
        type=lambda x: is_valid_output_file(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    in_loc = args.in_file
    out_dir = args.out_folder
    enable_post_filter = args.post_filter
    rejects_loc = args.rejects_loc

    if rejects_loc and not enable_post_filter:
        print(
            'WARNING: Post filter disabled. Rejects file will only contain'
            ' rejects from structuring stage'
        )

    # Set start time, to be able to print progress every 10 seconds
    last_log_time = time.time()

    process_data_file(in_loc, out_dir)
