# -*- coding: utf-8 -*-
"""
Script that structures, translates and post_processes harvested data stored
in json-lines files
"""
import os
import re
import json
from multiprocessing import Process, Queue
import argparse
from pathlib import Path
import copy
import logging
from logging import handlers

from metadata_ingestion import _loadcfg, structurers, translate, post_process

MEMORIZE = 5000

LOG_LEVEL = logging.INFO

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


def is_valid_processcount(parser, nprocesses):
    nprocesses = int(nprocesses)
    if nprocesses < 3:
        parser.error('nprocesses should be more than 2')
    else:
        return nprocesses


def get_process_logger(logging_queue):
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


def process_data(inqueue, outqueue, logqueue):
    """
    Process the list of data
    """
    logger = get_process_logger(logqueue)
    structurer_cache = {}

    while True:
        bdata = inqueue.get()

        if bdata.get('close'):
            if not inqueue.empty():
                logger.warning(
                    'Close command on queue size: {}!'.format(inqueue.qsize())
                )
            break

        dplatform_id = bdata['source']['id']

        # Since this only happens on the first access,
        # it's more efficient to handle the error, than
        # to check for the key
        try:
            structurer = structurer_cache[dplatform_id]
        except KeyError:
            s_kwargs = bdata['source']['structurer_kwargs']
            s_name = bdata['source']['structurer']
            structurer = getattr(structurers, s_name)(
                dplatform_id, **s_kwargs
            )
            structurer_cache[dplatform_id] = structurer

        processed_batch = []
        for line_nr, hdat in bdata['data']:
            try:
                # Structuring
                metadata = structurer.structure(hdat)

                if metadata.is_filtered:
                    continue
                else:
                    metadata.add_structured_legacy_fields()

                # Translation
                translated_entry =\
                    translate.single_entry(metadata.structured,
                                           dplatform_id)

                # Post Processing
                if post_process.is_filtered(translated_entry):
                    continue
                else:
                    post_process.optimize(translated_entry)
                    post_process.score(translated_entry)

            except Exception:
                logger.exception(
                    'The following exception occured at line {}'.format(
                        line_nr)
                    )
                continue

            processed_batch.append(translated_entry)

        outqueue.put({
            'data': processed_batch,
            'output': bdata['output']
        })


def iterate_jsonlines(fileloc):
    """
    Generator that returns the data from lines of a json-lines file
    """
    with open(fileloc, 'r', encoding='utf8') as jsonlinesfile:
        for line in jsonlinesfile:
            yield json.loads(line)


class FileHandles:
    """
    Object that manages filehandles, that new ones are created, old ones are
    closed, and also tracks the count for a filehandle

    Arguments:
        logger=None --- str: logging.Logger: If a logger is given, it's used
        to print total counts before closing a file, and warnings in case a
        file is reopened
    """

    def __init__(self, logger=None):
        self._data = {}
        self.maxopen = 20
        self.previously_closed = set()
        self.logger = logger

    def get(self, filepath):
        """
        Get the filehandle data (dict) for the given filepath (pathlib.Path).
        """
        name = filepath.name
        if name not in self._data:
            # Create new one, delete oldest if size is self.maxopen is exceeded
            if len(self._data) == self.maxopen:
                oldestkey = list(self._data.keys())[0]
                self._closefile(oldestkey)

            mode = 'w'
            if name in self.previously_closed:
                if self.logger is not None:
                    self.logger.warning(
                        'Reopened previously closed file: {}'.format(name)
                    )
                mode = 'a'
            self._data[name] = {
                'file': open(filepath, mode, encoding='utf8'),
                'count': 0
            }
        return self._data[name]

    def _closefile(self, name):
        """
        Close the file with the given name
        """
        data = self._data.pop(name)
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


def write_results(outqueue, logqueue):
    """
    Write the results that are on the outqueue to a file
    """
    logger = get_process_logger(logqueue)

    # Keys are filenames, contains a dict with 'file' (the filehandle) and
    # 'count' (the count saved to that handle)
    filehandles = FileHandles(logger=logger)
    while True:
        results = outqueue.get()

        if results.get('close'):
            if not outqueue.empty():
                logger.warning('Close command given on non-empty queue!')
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
        help="The size of a single batch that's pushed to a worker for processing (default=500)",
        type=int,
        default=500
    )
    aparser.add_argument(
        "--queuedbatches",
        help="Maximum number of batches on the input queue, ideally nprocesses-2 (default=7)",
        type=int,
        default=7
    )

    # Get arguments
    args = aparser.parse_args()
    in_dir = args.in_folder
    out_dir = args.out_folder
    process_count = args.nprocesses
    batch_size = args.batchsize
    queue_size = args.queuedbatches

    if in_dir == out_dir:
        aparser.error('Input directory cannot equal output directory!')

    # Configure logging
    # First: Set-up queue listener
    LOG_LOC = Path(out_dir, 'processing.log')
    logqueue = Queue()
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

    input_queue = Queue(queue_size)
    # Output queue size limit may not be necessary, because writing should
    # not be the bottleneck
    output_queue = Queue(queue_size * 2)

    # Initiate the worker processes
    workers = []
    for i in range(process_count - 2):
        p = Process(
            target=process_data, args=(input_queue, output_queue, logqueue),
            daemon=False
        )
        p.start()
        workers.append(p)

    # Initiate the writer process
    writer = Process(
        target=write_results, args=(output_queue, logqueue), daemon=False
    )
    writer.start()

    # Start reading data, and put it on the queue
    try:
        for path in file_paths:
            # Gather properties
            sourceid = filename_regex.match(path.name).group(1)
            source = [s for s in sources if s['id'] == sourceid][0]

            properties = {
                'source': {
                    'id': sourceid,
                    'structurer_kwargs': source.get('structurer_kwargs', {}),
                    # Don't pass the object, it needs to be pickled each time
                    # rather each thread keeps a cache of structurers per
                    # sourceid
                    'structurer': source['structurer']
                },
                'output': Path(out_dir, path.name)
            }

            # Read data in batches, and put on input queue. Since it has a maxsize,
            # it will block if enough data is read
            batch = []
            linenr = 0
            for hdat in iterate_jsonlines(path):
                linenr += 1
                batch.append((linenr, hdat))
                if len(batch) == batch_size:
                    batch_data = copy.deepcopy(properties)
                    batch_data['data'] = batch
                    input_queue.put(batch_data)
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
                    batch_data = copy.deepcopy(properties)
                    batch_data['data'] = batch
                    input_queue.put(batch_data)
                    batch = []

        for w in workers:
            input_queue.put({'close': True})

        # After everyting has been pushed, wait for the worker-processes to join
        for w in workers:
            w.join()

        output_queue.put({'close': True})
        # Finally, wait for the write process to join
        writer.join()
    finally:
        listener.stop()
