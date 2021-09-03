# -*- coding: utf-8 -*-
"""
Configuration loader sub-module

Functions to load various configuration files. Set-up as a seperate module to
allow for flexibly switching from local filesystem to online filesystems or
databases
"""
import re
from pathlib import Path

from metadata_ingestion import dataio
from metadata_ingestion.settings import INGESTION_CONF_DIR

date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def sources():
    """
    Returns the list of metadata sources
    """
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'sources.yaml'))


def postfilters():
    """
    Returns the list of metadata sources
    """
    # Convert lowest level to sets
    postfilters = dataio.loadyaml(
        Path(INGESTION_CONF_DIR, r'postfilters.yaml')
    )
    postfilters_sets = {}
    for k, v in postfilters.items():
        if isinstance(v, dict):
            newvalue = {sk: set(sv) for sk, sv in v.items()}
        else:
            newvalue = set(v)
        postfilters_sets[k] = newvalue

    return postfilters_sets


def translators():
    """
    Returns the translators.yaml config data
    """
    return dataio.loadyaml(
        Path(INGESTION_CONF_DIR, r'translators.yaml')
    )


def subject_scheme():
    """
    Returns the subject scheme data
    """
    return dataio.loadjson(Path(INGESTION_CONF_DIR, r'subject_scheme.json'))


def file_format_mapping():
    return dataio.loadjson(Path(INGESTION_CONF_DIR, r'ff_mapping.json'))


def language_mapping():
    return dataio.loadjson(Path(INGESTION_CONF_DIR, r'lang_mapping.json'))


def epsg_codes():
    return dataio.loadjson(Path(INGESTION_CONF_DIR, r'epsg_codes.json'))


def name_to_epsg():
    return dataio.loadjson(Path(INGESTION_CONF_DIR, r'name2epsg.json'))
