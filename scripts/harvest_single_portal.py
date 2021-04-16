# -*- coding: utf-8 -*-
"""
Script to harvest data using a single harvester
"""
import asyncio
import logging
from logging import handlers
from pathlib import Path
import argparse

from metadata_ingestion import _loadcfg
from metadata_ingestion import harvesters

sources = _loadcfg.sources()


def is_valid_folder(parser, dirloc):
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_portal_id(parser, portal_id):
    portal_list = [s for s in sources if s['id'] == portal_id]
    if len(portal_list) == 1:
        return portal_id
    else:
        parser.error('The portal id {} does not exist'.format(portal_id))


def is_valid_log_level(parser, log_level):
    log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level in log_levels:
        return log_level
    else:
        parser.error('Log level {} not one of {}'.format(
            log_level, str(log_levels)
            )
        )


async def main(h):
    await h.run()

if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description=("Harvest the data of a single portal")
    )
    aparser.add_argument(
        "portal_id",
        help="The unique identifier of the portal to harvest data from",
        type=lambda x: is_valid_portal_id(aparser, x)
    )
    aparser.add_argument(
        "folder",
        help="Folder to store output data and logs",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "--log-level",
        help="Set the log level (e.g. DEBUG). Default=INFO",
        default="INFO",
        dest="log_level",
        type=lambda x: is_valid_log_level(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    portal_id = args.portal_id
    portal_folder = args.folder

    portal_metadata = [s for s in sources if s['id'] == portal_id][0]

    harvester = getattr(harvesters, portal_metadata['harvester'])(
        id_=portal_id,
        output_path=portal_folder,
        **portal_metadata['harvester_kwargs']
    )

    # Enable basic logging
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, args.log_level))
    handler = handlers.RotatingFileHandler(
        Path(portal_folder, 'harvest.log'),
        maxBytes=1048576,
        backupCount=5
        )
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-10s | %(name)s: %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Run the harvester
    asyncio.run(main(harvester))
