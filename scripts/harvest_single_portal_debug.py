# -*- coding: utf-8 -*-
"""
Script to harvest data using the async harvesters (TESTING ONLY)
"""
import asyncio
import logging
from logging import handlers
from pathlib import Path

from metadata_ingestion import harvest as aio_harvest  # Module was renamed


async def main(h):
    await h.run()

if __name__ == "__main__":
    portal_folder = '/home/brouwer/Downloads/testing'
    harvester = aio_harvest.GeonodeHarvester(
        id_='masdap_mw',
        output_path=portal_folder,
        api_url='http://www.masdap.mw/',
        limit=100
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
    asyncio.run(main(harvester))
