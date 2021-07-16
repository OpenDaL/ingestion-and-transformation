# Metadata ingestion python module
Python module with functions to ingest metadata from several online APIs

## Requirements
This package has been tested using Python 3.7.  The most recent versions of all
dependencies are installed when running `pip install ./` from this directory.

## Sub-packages / modules
The structure of this package is as follows:

* __metadata_ingestion__:
    * __[harvest](metadata_ingestion/harvest.py)__: Functions to harvest the
    raw data from several API types, and store the data as json-lines files
    * __[structure](metadata_ingestion/structure.py)__: Functions to structure
    & flatten the raw API data, making it ready for the translation stage
    * __[translate](metadata_ingestion/translate.py)__: Functions to translate
    structured data into the final metadata format
    * __[settings](metadata_ingestion/settings.py)__: Package settings
    * __[exceptions](metadata_ingestion/exceptions.py)__: Custom exceptions
    used by some of the sub-modules
    * __[analyze](metadata_ingestion/analyze.py)__: Functions to analyze
    raw/structured data, to help in the translation stage
    * _[\_aux](metadata_ingestion/_aux.py)_: Auxiliary/Helper functions used by
    multiple sub-modules
    * _[\dataio](metadata_ingestion/dataio.py)_: Functions for IO to local
    file system
    * _[\_loadcfg](metadata_ingestion/_loadcfg.py)_: Functions to load
    configuration files used by the package

## Use examples
Examples of using some of the functions of each sub-module are given below:

#### metadata_ingestion.harvest
To ingest data from an online API, and write the data to a json-lines file, use the `.to_file()` function:
```python
from metadata_ingestion import harvest

harvest.to_file('platform_id', 'https://opendata.portal/api', 'CKAN3',
                r'/data/harvested_data.jl')
```

#### metadata_ingestion.structure
To structure raw ingested data from a file, use the `.ingested_data()` function:
```python
from metadata_ingestion import structure

structure.ingested_data(r'/data/harvested_data.jl',
                        r'/data/structured_data.jl',
                        'CKAN3')
```

#### metadata_ingestion.translate
To translate a single structured entry, use the `.single_entry()` function:
```python
from metadata_ingestion import translate
import json

translated_data = []
with open(r'/data/structured_data.jl', 'r', encoding='utf8') as jsonlinesfile:
    for line in jsonlinesfile:
        structured = json.loads(line)
        translated = translate.single_entry(structured)
        translated_data.append(translated)
```
