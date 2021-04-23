# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name="metadata_ingestion",
    version="0.5.8",
    packages=find_packages(),
    install_requires=[
        'requests>=2.18.4',
        'xmltodict>=0.11.0',
        'unidecode>=1.0.22',
        'html2text>=2018.1.9',
        'dateparser>=0.7.0',
        'shapely>=1.6.4',
        'pyyaml>=5.1.1',
        'aiohttp>=3.5.4',
        'cloudscraper>=1.2.28',
        'fastjsonschema>=2.15.0,<3.0.0'
        ],
    package_data={
        'metadata_ingestion': ['default_config.yaml']
    },
    zip_safe=False,  # This allows settings.py to be edited after install
    author="Tom Brouwer",
    author_email="tombrouwer@outlook.com",
    description="Code to ingestion metadata for the DataClopedia platform",
    license="TBD",
)
