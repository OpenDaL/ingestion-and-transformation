# -*- coding: utf-8 -*-
"""
Script to harvest data using the async harvesters (TESTING ONLY)
"""
import asyncio
import logging
from logging import handlers
from pathlib import Path
import re
import argparse

from metadata_ingestion import _loadcfg
from metadata_ingestion import harvesters


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


async def run_harvest_tasks(q):
    """
    Runs harvesters on a queue
    """
    while True:
        harvester_instances = await q.get()
        for harvester in harvester_instances:
            await harvester.run()
        q.task_done()


async def produce_harvest_tasks(q, output_folder):
    """
    Create the harvest tasks, and put them on the queue
    """
    domain_regex = re.compile('https?://([^/]+)')
    sources = _loadcfg.sources()
    # Sort sources by length
    sorted_sources = sorted(sources, key=lambda k: k['count'], reverse=True)
    # Now sort per domain, because we don't want concurrent requests to same
    # domain, since this can lead to huge timeouts/service failures
    per_domain = {}
    for source in sorted_sources:
        harvester = source.get('harvester')
        if harvester is not None:
            h = getattr(harvesters, harvester)(
                id_=source['id'],
                output_path=output_folder,
                **source['harvester_kwargs']
            )
            api_url = source['harvester_kwargs']['api_url']
            domain = domain_regex.match(api_url).group(1)
            if domain not in per_domain:
                per_domain[domain] = [h]
            else:
                per_domain[domain].append(h)

    for harvester_list in per_domain.values():
        await q.put(harvester_list)


async def main():
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Harvest data from all configured portals"
    )
    aparser.add_argument(
        "folder",
        help="The folder to save harvested data and logs",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "--override-sources",
        help="Override the package default sources.yaml using this file",
        dest='sources_loc',
        type=lambda x: is_valid_file(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    portal_folder = args.folder
    sources_loc = args.sources_loc

    if sources_loc:
        _loadcfg.st.SRCS_CFG_LOC = sources_loc

    # Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = handlers.RotatingFileHandler(
        Path(portal_folder, 'async_harvest.log'),
        maxBytes=1048576,
        backupCount=5
        )
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Create queue and spool up producer and consumers
    q = asyncio.Queue()
    producer = asyncio.create_task(produce_harvest_tasks(q, portal_folder))
    consumers = [asyncio.create_task(run_harvest_tasks(q)) for
                 i in range(6)]  # 6 Consumers running
    await asyncio.gather(producer)
    await q.join()
    for c in consumers:
        c.cancel()

if __name__ == "__main__":
    # Run the harvester
    asyncio.run(main())
