# -*- coding: utf-8 -*-
"""
Script that structures, translates and post_processes harvested data stored
in json-lines files
"""
import re
from pathlib import Path
import time
import argparse

from metadata_ingestion import (
    structurers, translators, post_processors, resource, dataio
)

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


def process_data_file(
        input_loc, output_dir, default_steps, store_empty=False
        ):
    """
    Processes a single file of data, halts on errors
    """
    print('Start processing')

    filename = input_loc.name
    source_id = filename_regex.match(filename).group(1)
    out_loc = Path(output_dir, filename)

    structurer = structurers.get_structurer(source_id)

    processing_steps = [structurer.structure] + default_steps

    write_queue = []
    write_mode = 'w'
    count = 0
    print_time = time.time()
    for i, item in enumerate(dataio.iterate_jsonlines(input_loc)):
        count += 1
        try:
            metadata = resource.ResourceMetadata(item)
            for apply_step in processing_steps:
                apply_step(metadata)
                if metadata.is_filtered:
                    if store_empty:
                        write_queue.append(None)
                    break
            else:
                write_queue.append(
                    metadata.get_full_data()
                )
        except Exception:
            print('At index {}:'.format(i))
            raise

        if (time.time() - print_time) > 10:
            print('Processed {} items'.format(count))
            print_time = time.time()

        if len(write_queue) == MEMORIZE:
            dataio.savejsonlines(write_queue, out_loc, mode=write_mode)
            write_mode = 'a'
            write_queue = []
    else:
        dataio.savejsonlines(write_queue, out_loc, mode=write_mode)


if __name__ == '__main__':
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Process data from a single portal into the OpenDaL format"
    )
    aparser.add_argument(
        "input",
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
        dest='enable_filters'
    )

    aparser.add_argument(
        "--store-empty",
        help="If items are filtered, store 'null'",
        action="store_true"
    )

    aparser.add_argument(
        "--batch-size",
        help="Process data in batches of this size (default=2000)",
        type=int,
        default=2000
    )

    # Get arguments
    args = aparser.parse_args()

    translator = translators.MetadataTranslator()
    post_processor = post_processors.MetadataPostProcessor(
        enable_filters=args.enable_filters
    )

    default_steps = [
        translator.translate,
        post_processor.post_process
    ]

    # Set start time, to be able to print progress every 10 seconds
    last_log_time = time.time()

    MEMORIZE = args.batch_size

    process_data_file(
        args.input, args.out_folder, default_steps,
        store_empty=args.store_empty,
    )
