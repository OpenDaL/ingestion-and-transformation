# -*- coding: utf-8 -*-
"""
Module for loading configuration files

This module contains all functions related to the loading and parsing of
configuration files

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
from pathlib import Path

from metadata_ingestion import dataio
from metadata_ingestion.settings import INGESTION_CONF_DIR


def sources() -> list[dict]:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'sources.yaml'))


def postfilters() -> dict:
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


def translators() -> dict:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'translators.yaml'))


def subject_scheme() -> dict:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'subject_scheme.yaml'))


def file_format_mapping() -> dict:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'ff_mapping.yaml'))


def language_mapping() -> dict:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'lang_mapping.yaml'))


def epsg_codes() -> list[int]:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'epsg_codes.yaml'))


def name_to_epsg() -> dict:
    return dataio.loadyaml(Path(INGESTION_CONF_DIR, r'name2epsg.yaml'))
