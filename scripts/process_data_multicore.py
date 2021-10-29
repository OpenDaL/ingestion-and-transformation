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
import multiprocessing
import argparse
from pathlib import Path
import logging
from logging import handlers
from typing import TextIO

from metadata_ingestion import (
    structurers, translators, post_processors, resource, dataio
)

MEMORIZE = 5000

LOG_LEVEL = logging.INFO

filename_regex = re.compile(
    r'(.*)_\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z?\.(jl|jsonl)$'
)


class StructurerCache():
    """
    Cache to store initialized structurers

    Storing them prevents having to construct new objects for the processing
    of each item
    """
    def __init__(self) -> None:
        self._structurers = {}

    def get(self, source_id: str) -> structurers.Structurer:
        # Using try except is fastest, since it will only fail once, for
        # the first item processed
        try:
            return self._structurers[source_id]
        except KeyError:
            self._structurers[source_id] = structurers.get_structurer(
                source_id
            )
            return self._structurers[source_id]


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """Check if the provided folder is valid"""
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_processcount(
        parser: argparse.ArgumentParser, nprocesses: str
        ) -> int:
    """Parse and validate the provided number of processes"""
    nprocesses = int(nprocesses)
    if nprocesses < 3:
        parser.error('nprocesses should be more than 2')
    else:
        return nprocesses


def get_process_logger(logging_queue: multiprocessing.Queue) -> logging.Logger:
    """
    Get the root logger for this process, all set-up to log to the given
    logging queue using the QueueHandler
    """
    # Set-up the logger
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)
    handler = handlers.QueueHandler(logging_queue)
    handler.setLevel(LOG_LEVEL)
    logger.addHandler(handler)

    return logger


def process_data(
        inqueue: multiprocessing.Queue, outqueue: multiprocessing.Queue,
        logqueue: multiprocessing.Queue
        ):
    """
    Process the list of data
    """
    logger = get_process_logger(logqueue)
    structurer_cache = StructurerCache()

    translator = translators.MetadataTranslator()
    post_processor = post_processors.MetadataPostProcessor()

    default_steps = [
        translator.translate,
        post_processor.post_process
    ]

    while True:
        bdata = inqueue.get()

        if bdata.get('close'):
            logger.info(
                'Close command received at queue size: {}!'.format(
                    inqueue.qsize()
                ),
            )
            break

        structurer = structurer_cache.get(bdata['source']['id'])

        processing_steps = [structurer.structure] + default_steps

        processed_batch = []
        for line_nr, hdat in bdata['data']:
            try:
                metadata = resource.ResourceMetadata(hdat)
                for apply_step in processing_steps:
                    apply_step(metadata)
                    if metadata.is_filtered:
                        break
                else:
                    processed_batch.append(
                        metadata.get_full_data()
                    )
            except Exception:
                logger.exception(
                    'The following exception occured at line'
                    ' {} in file {}'.format(
                        line_nr,
                        bdata['output'].name
                    )
                )
                continue

        outqueue.put({
            'data': processed_batch,
            'output': bdata['output']
        })


class FileHandles:
    """
    Object that manages filehandles, that new ones are created, old ones are
    closed, and also tracks the count for a filehandle
    """
    def __init__(self, *, logger: logging.Logger):
        """
        Initializes the FileHandles instance

        Args:
            logger:
                The logger to log closing and opening of filehandles to
        """
        self._data = {}
        self.maxopen = 20
        self.previously_closed = set()
        self.logger = logger

    def _addfile(self, filepath: Path):
        """Add a file to the filehandles cache"""
        if len(self._data) == self.maxopen:
            oldestkey = list(self._data.keys())[0]
            self._closefile(oldestkey)

        if filepath.name in self.previously_closed:
            self.logger.warning(
                'Reopened previously closed file: {}'.format(filepath.name)
            )
            mode = 'a'
        else:
            mode = 'w'

        self._data[filepath.name] = {
            'file': open(filepath, mode, encoding='utf8'),
            'count': 0
        }

    def get(self, filepath: Path) -> TextIO:
        """
        Get the filehandle data (dict) for the given filepath (pathlib.Path).
        """
        name = filepath.name
        try:
            return self._data[name]
        except KeyError:
            self._addfile(filepath)
            return self._data[name]

    def _closefile(self, name: str):
        """
        Close the file with the given name
        """
        data = self._data.pop(name)
        data['file'].close()
        if self.logger is not None:
            self.logger.info(
                'File {} closed, total stored: {}'.format(
                    name,
                    data['count']
                )
            )
        self.previously_closed.add(name)

    def closeall(self):
        """
        Close all filehandles
        """
        for name in list(self._data.keys()):
            self._closefile(name)


def write_results(
        outqueue: multiprocessing.Queue, logqueue: multiprocessing.Queue
        ):
    """
    Write the results that are on the outqueue to a file
    """
    logger = get_process_logger(logqueue)

    filehandles = FileHandles(logger=logger)
    while True:
        results = outqueue.get()

        if results.get('close'):
            if not outqueue.empty():
                logger.warning(
                    'Close command given on non-empty output queue!'
                )
            break

        data = results['data']
        output_path = results['output']
        filehandle = filehandles.get(output_path)

        for dat in data:
            filehandle['file'].write(
                json.dumps(dat, ensure_ascii=False) + '\n'
            )

        filehandle['count'] += len(data)

    filehandles.closeall()


if __name__ == '__main__':
    # Since the base process uses multithreading for the queuelistener, use the
    # spawn method (Already used by default on Windows and Mac)
    multiprocessing.set_start_method('spawn')

    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Process harvested data into the OpenDaL format"
    )
    aparser.add_argument(
        "in_folder",
        help="The folder that contains the input (harvested) data",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "out_folder",
        help="The folder to save the output (processed) data",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "--nprocesses",
        help="The number of processes to spawn (minimum is 2, default=9)",
        type=lambda x: is_valid_processcount(aparser, x),
        default=9
    )
    aparser.add_argument(
        "--batchsize",
        help=(
            "The size of a single batch that's pushed to a worker for "
            "processing (default=500)"
        ),
        type=int,
        default=500
    )

    # Get arguments
    args = aparser.parse_args()
    in_dir = args.in_folder
    out_dir = args.out_folder
    process_count = args.nprocesses
    batch_size = args.batchsize
    queue_size = process_count - 2

    if in_dir == out_dir:
        aparser.error('Input directory cannot equal output directory!')

    # Configure logging
    # First: Set-up queue listener
    LOG_LOC = Path(out_dir, 'processing.log')
    logqueue = multiprocessing.Queue()
    filehandler = handlers.RotatingFileHandler(
        LOG_LOC, maxBytes=1048576, backupCount=4
    )
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    filehandler.setFormatter(formatter)
    filehandler.setLevel(LOG_LEVEL)
    listener = handlers.QueueListener(logqueue, filehandler)
    listener.start()  # runs in new thread

    # Second: Set logging for main process
    logger = get_process_logger(logqueue)

    all_files = os.listdir(in_dir)
    # Get filenames and file sizes
    file_paths = [
        p for p in in_dir.iterdir()
        if p.is_file() and filename_regex.match(p.name) is not None
    ]

    input_queue = multiprocessing.Queue(queue_size)
    # Output queue size limit may not be necessary, because writing should
    # not be the bottleneck
    output_queue = multiprocessing.Queue(queue_size * 2)

    # Initiate the worker processes
    workers = []
    for i in range(process_count - 2):
        p = multiprocessing.Process(
            target=process_data, args=(input_queue, output_queue, logqueue),
            daemon=False
        )
        p.start()
        workers.append(p)

    # Initiate the writer process
    writer = multiprocessing.Process(
        target=write_results, args=(output_queue, logqueue), daemon=False
    )
    writer.start()

    # Start reading data, and put it on the queue
    try:
        for path in file_paths:
            sourceid = filename_regex.match(path.name).group(1)

            properties = {
                'source': {
                    'id': sourceid,
                },
                'output': Path(out_dir, path.name),
            }

            # Read data in batches, and put on input queue. Since it has a
            # maxsize, it will block if enough data is read
            batch = []
            linenr = 0
            for hdat in dataio.iterate_jsonlines(path):
                linenr += 1
                batch.append((linenr, hdat))
                if len(batch) == batch_size:
                    input_queue.put({
                        **properties,
                        'data': batch,
                    })
                    batch = []
                if linenr % 5000 == 0:
                    logger.info(
                        'File {}: put {} items on queue'.format(
                            path.name,
                            linenr
                        )
                    )
            else:
                logger.info(
                    'File {}: total of {} items read'.format(
                        path.name,
                        linenr
                    )
                )
                if batch:
                    input_queue.put({
                        **properties,
                        'data': batch,
                    })
                    batch = []

        for w in workers:
            input_queue.put({'close': True})

        # After everyting has been pushed, wait for the worker-processes to
        # join
        for w in workers:
            w.join()

        output_queue.put({'close': True})
        # Finally, wait for the write process to join
        writer.join()
    finally:
        listener.stop()
