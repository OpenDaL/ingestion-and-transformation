# -*- coding: utf-8 -*-
"""
SETTINGS FILE

The settings used by all functions are defined in this file
"""
import logging
from pathlib import Path
import os

import yaml

logger = logging.getLogger(__name__)


def get_config_loc():
    """
    Returns the location of the configuration file (either pacakge default or
    the one in $CONFIG_DIR. Returns a pathlib.Path
    """
    config_loc = Path(Path(__file__).parent, 'default_config.yaml')

    # Check if there is a system level config
    config_dir = os.environ.get('CONFIG_DIR')
    if config_dir is not None:
        config_path = Path(config_dir, 'opendal_ingestion.yaml')
        if config_path.is_file():
            config_loc = config_path
            logger.info('Using local config file found at {}'.format(
                config_path.as_posix())
            )

    return config_loc


def load_config():
    """
    Load the opendal_ingestion.yaml in the CONFIG_DIR if available, otherwise
    load the default config of the package
    """
    global INGESTION_CONF_DIR, RQ_TIMEOUT, DATETIME_FORMAT, WRITE_PER, DELAYS,\
        CKAN3_AMT_ALLOWED_MISS, REP_TEXTKEY, DATE_FORMAT, NOW_PDAYS

    config_loc = get_config_loc()
    with open(config_loc, 'r', encoding="utf8") as configfile:
        cdata = yaml.safe_load(configfile)

    # Config dir
    INGESTION_CONF_DIR = cdata['config_dir']
    if not Path(INGESTION_CONF_DIR).is_dir():
        raise ValueError('The config_dir in the configuration file is not valid')

    # Harvest settings
    RQ_TIMEOUT = cdata['max_timeout']
    DATETIME_FORMAT = cdata['datetime_format']
    WRITE_PER = cdata['write_per']
    DELAYS = cdata['delays']

    CKAN3_AMT_ALLOWED_MISS = cdata['ckan3']['max_amount_missing']

    # Data Structuring
    REP_TEXTKEY = cdata['rep_textkey']

    # Data Translation
    DATE_FORMAT = cdata['date_format']
    NOW_PDAYS = cdata['now_add_days']


# Run config load function
load_config()

# Set certificate dir
CERT_DIR = Path(INGESTION_CONF_DIR, r'certificates')

# Set config file locations
SRCS_CFG_LOC = Path(INGESTION_CONF_DIR, r'sources.yaml')
TRANSL_CFG_LOC = Path(INGESTION_CONF_DIR, r'translation.json')
PSTFLTR_LOC = Path(INGESTION_CONF_DIR, r'postfilters.yaml')
FILTS_CFG_LOC = Path(INGESTION_CONF_DIR, r'filters.json')
TRULES_CFG_LOC = Path(INGESTION_CONF_DIR, r'translation_rules.json')
SUBJECT_SCHEME_LOC = Path(INGESTION_CONF_DIR, r'subject_scheme.json')
FF_MAPPING_LOC = Path(INGESTION_CONF_DIR, r'ff_mapping.json')
LANG_MAPPING_LOC = Path(INGESTION_CONF_DIR, r'lang_mapping.json')
EPSG_LOC = Path(INGESTION_CONF_DIR, r'epsg_codes.json')
EPSG_NAMES_LOC = Path(INGESTION_CONF_DIR, r'name2epsg.json')
