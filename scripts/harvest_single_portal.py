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


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str) -> Path:
    """
    Check if the given location is a valid folder
    """
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def is_valid_source_id(parser: argparse.ArgumentParser, source_id: str) -> str:
    """
    Check if the provided source_id is valid
    """
    portal_list = [s for s in sources if s['id'] == source_id]
    if len(portal_list) == 1:
        return source_id
    else:
        parser.error('The portal id {} does not exist'.format(source_id))


def is_valid_log_level(parser: argparse.ArgumentParser, log_level: str) -> str:
    """
    Check if the provided log level is valid
    """
    log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level in log_levels:
        return log_level
    else:
        parser.error('Log level {} not one of {}'.format(
            log_level, str(log_levels)
            )
        )


if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description=("Harvest the data of a single portal")
    )
    aparser.add_argument(
        "source_id",
        help="The unique identifier of the portal to harvest data from",
        type=lambda x: is_valid_source_id(aparser, x)
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
    source_id = args.source_id
    portal_folder = args.folder

    portal_metadata = [s for s in sources if s['id'] == source_id][0]

    harvester = getattr(harvesters, portal_metadata['harvester'])(
        id_=source_id,
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
    asyncio.run(harvester.run())
