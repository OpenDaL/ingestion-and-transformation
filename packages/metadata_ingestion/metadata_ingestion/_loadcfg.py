# -*- coding: utf-8 -*-
"""
Configuration loader sub-module

Functions to load various configuration files. Set-up as a seperate module to
allow for flexibly switching from local filesystem to online filesystems or
databases
"""
import datetime
import re
from pathlib import Path

from metadata_ingestion import _dataio
from metadata_ingestion.settings import INGESTION_CONF_DIR

date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def sources():
    """
    Returns the list of metadata sources
    """
    return _dataio.loadyaml(Path(INGESTION_CONF_DIR, r'sources.yaml'))


def postfilters():
    """
    Returns the list of metadata sources
    """
    # Convert lowest level to sets
    postfilters = _dataio.loadyaml(
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


def translation():
    """
    Returns the translation configuration data
    """
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'translation.json'))


def translation_rules():
    """
    Returns the translation rules
    """
    def str_to_date(str_):
        if str_ == 'now':
            date = datetime.datetime.now(tz=datetime.timezone.utc)
        else:
            date = datetime.datetime.strptime(str_, '%Y-%m-%d').replace(
                tzinfo=datetime.timezone.utc
            )

        return date

    def convert_date_requirements(requirements):
        date_keys = ['lt', 'lte', 'gt', 'gte']
        for key in date_keys:
            if key in requirements:
                ddata = requirements[key]
                # The string 'now' should be loaded by a function once it loads
                # , so that it does not become outdated...
                if isinstance(ddata, str) and date_regex.match(ddata):
                    requirements[key] = str_to_date(ddata)

    data = _dataio.loadjson(
        Path(INGESTION_CONF_DIR, r'translation_rules.json')
    )

    # Look for 'than' and 'greater than' dates, and convert them to datetime
    for parent, pdata in data.items():
        convert_date_requirements(pdata)
        children = pdata.get('children')
        if children is not None:
            for child, cdata in children.items():
                convert_date_requirements(cdata)

    return data


def filters():
    """
    Returns the filter configuration data
    """
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'filters.json'))


def subject_scheme():
    """
    Returns the subject scheme data
    """
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'subject_scheme.json'))


def file_format_mapping():
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'ff_mapping.json'))


def language_mapping():
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'lang_mapping.json'))


def epsg_codes():
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'epsg_codes.json'))


def name_to_epsg():
    return _dataio.loadjson(Path(INGESTION_CONF_DIR, r'name2epsg.json'))
