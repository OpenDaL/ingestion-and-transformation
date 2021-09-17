# -*- coding: utf-8 -*-
"""
Settings Module

This contains all systemwide settings used throughout the package and by the
scripts
"""
import logging
from pathlib import Path
import os
from typing import Union

import yaml

logger = logging.getLogger(__name__)


def get_config_loc() -> Union[str, Path]:
    """
    Returns the location of the configuration file (either pacakge default or
    the one in $CONFIG_DIR. Returns a pathlib.Path

    Raises:
        ValueError:
            If the CONFIG_DIR environment variable is not defined, or the
            opendal_ingestion.yaml file is not in the CONFIG_DIR

    Returns:
        Location of the config file
    """
    # Check if there is a system level config
    config_dir = os.environ.get('CONFIG_DIR')
    config_loc = None
    if config_dir is not None:
        config_path = Path(config_dir, 'opendal_ingestion.yaml')
        if config_path.is_file():
            config_loc = config_path
            logger.info('Using local config file found at {}'.format(
                config_path.as_posix())
            )
        else:
            raise ValueError('opendal_ingestion.yaml not found in $CONFIG_DIR')
    else:
        raise ValueError('$CONFIG_DIR environment variable not set')

    return config_loc


def load_config():
    """
    Loads the data from opendal_ingestion.yaml to module level constants
    """
    global INGESTION_CONF_DIR, DELAYS, REP_TEXTKEY, DATE_FORMAT, NOW_PDAYS

    config_loc = get_config_loc()
    with open(config_loc, 'r', encoding="utf8") as configfile:
        cdata = yaml.safe_load(configfile)

    # Config dir
    INGESTION_CONF_DIR = cdata['config_dir']
    if not Path(INGESTION_CONF_DIR).is_dir():
        raise ValueError(
            'The config_dir in the configuration file is not valid'
        )

    # Harvest settings
    DELAYS = cdata['delays']

    # Data Structuring
    REP_TEXTKEY = cdata['rep_textkey']

    # Data Translation
    DATE_FORMAT = cdata['date_format']
    NOW_PDAYS = cdata['now_add_days']


# Run config load function
load_config()

# Set certificate dir
CERT_DIR = Path(INGESTION_CONF_DIR, r'certificates')
