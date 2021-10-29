# -*- coding: utf-8 -*-
"""
Script to harvest data all data for the sources defined in sources.yaml

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
import asyncio
import logging
from logging import handlers
from pathlib import Path
import re
import argparse
from typing import Union

from metadata_ingestion import _loadcfg
from metadata_ingestion import harvesters


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """
    Checks if the given dirloc points to an existing directory
    """
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_file(parser: argparse.ArgumentParser, fileloc: str) -> Path:
    """
    Checks if the given fileloc points to an existing file
    """
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    else:
        return path


async def run_harvest_tasks(q: asyncio.Queue):
    """
    Runs harvesters on a queue
    """
    while True:
        harvester_instances = await q.get()
        for harvester in harvester_instances:
            await harvester.run()
        q.task_done()


async def produce_harvest_tasks(
        q: asyncio.Queue, output_folder: Union[Path, str]
        ):
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
        help="Override the default sources.yaml using this file",
        dest='sources_loc',
        type=lambda x: is_valid_file(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    portal_folder = args.folder
    sources_loc = args.sources_loc

    if sources_loc:
        _loadcfg.st.SRCS_CFG_LOC = sources_loc

    # Set a rotating file handler logger
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
