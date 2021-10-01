# -*- coding: utf-8 -*-
"""
Script to harvest data using the async harvesters (TESTING ONLY)
"""
import asyncio
import logging
from logging import handlers
from pathlib import Path
import argparse

from metadata_ingestion import harvesters, dataio


def load_harvester_kwargs(
        parser: argparse.ArgumentParser, fileloc: str
        ) -> dict:
    """
    Checks if the given fileloc is a valid JSON file, and load the
    kwargs
    """
    path = Path(fileloc)
    if not path.is_file():
        parser.error('The file {} does not exist'.format(fileloc))
    elif not path.name.endswith('.json'):
        parser.error('The file {} is not a JSON file'.format(fileloc))
    else:
        return dataio.loadjson(path)


def is_valid_output_folder(
        parser: argparse.ArgumentParser, dirloc: str
        ) -> Path:
    """
    Checks if the given output location is in a valid directory
    """
    outputpath = Path(dirloc)
    if not outputpath.is_dir():
        parser.error('The output directory does not exist')
    else:
        return outputpath


if __name__ == "__main__":
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description=(
            "Harvest from a single portal that's not yet configured in "
            "sources.yaml"
        ),
    )
    aparser.add_argument(
        "harvester",
        help="The Classname of the harvester to use",
        type=str
    )
    aparser.add_argument(
        "harvester_kwargs_json",
        help="Json file with the harvester kwargs",
        type=lambda x: load_harvester_kwargs(aparser, x),
    )
    aparser.add_argument(
        "output_folder",
        help="The output folder to store the harvested data and log",
        type=lambda x: is_valid_output_folder(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()

    Harvester = getattr(harvesters, args.harvester)

    portal_folder = args.output_folder
    harvester = Harvester(
        id_='debug_harvest',
        output_path=portal_folder,
        **args.harvester_kwargs_json
    )

    # Enable basic logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
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
