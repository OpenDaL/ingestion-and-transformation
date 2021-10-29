# OpenDaL Metadata Ingestion and Transformation
This repository contains all code and logic for collecting and translating data
from a variety of Open Data portals, and offers the following main functionality
related to metadata of datasets from open data portals:

* **Harvesting**: Taking in data from a range of different Websites/APIs in a
  range of different formats
* **Structuring**: Flattening the the harvested data, to get a dictionary of
  key/value combinations describing different aspects of a dataset
* **translation**: Translate the flattened data from the structuring stage into
  a single standardized format

The translated data that is created, can then be indexed, and made available in
a search engine, such as ElasticSearch.

## 1. Contents
This repository contains:

1. The ['metadata_ingestion' python package](packages/metadata_ingestion/)
2. [Scripts](scripts/) for the harvesting, analysis, structuring and
   translation of data. These use the above package

## 2. Usage

### 2.1 Dependencies
The code in this repository was tested in combination with Python 3.9 on
Debian based linux distributions. It should also work on Windows, but this is
untested.

### 2.2 Installation & Configuration
If you have Python 3.9 in your path, you can run [install_env.sh](install_env.sh)
to install the Python environment to the '.env' directory inside this
repository.

To activate the environment, you can use `source activate_env.sh`.

To be able to harvest and translate data, you will need to
1. Define a $CONFIG_DIR variable, holding the directory that contains the
   'opendal_ingestion.yaml' configuration file.
2. Copy the [configuration template](config/opendal_ingestion.TEMPLATE.yaml) to
   the given directory, rename to 'opendal_ingestion.yaml', and configure the
   variables in this file.

This configuration file has a 'config_dir' variable, which should point to a
directory that contains specific configuration for the 'metadata_ingestion'
package. This directory should contain the following files

* **sources.yaml**: Contains the details of the sources to harvest data from,
  and properties to use for the 'harvester' and 'structurer' that process the
  data for each source. ([schema](config/schemas/sources.yaml))
* **translators.yaml**: Contains the configuration
  (args/keyword args) for the [translators](packages/metadata_ingestion/metadata_ingestion/translators.py). ([schema](config/schemas/translators.yaml))
* **postfilters.yaml**: Contains the configuration for the post-filtering of
  data ([schema](config/schemas/postfilters.yaml))
* **subject_scheme.yaml**: Contains the subject data, including the text used
  to translate subjects from the source data. ([schema](config/schemas/subject_scheme.yaml))
* **ff_mapping.yaml**: A mapping from all types of file format descriptions (
  e.g. mime-types, file types) to standardised letter codes ([schema](config/schemas/ff_mapping.yaml))
* **lang_mapping.yaml**: A mapping from all kind of descriptions of languages
  to two letter language codes ([schema](config/schemas/lang_mapping.yaml))
* **epsg_codes.yaml**: A list of integer codes, that refer to valid EPSG
  coordinate system projections ([schema](config/schemas/epsg_codes.yaml))
* **name2epsg.yaml**: A mapping from a variety of different strings
  describing a coordinate system, to integer EPSG codes.
  ([schema](config/schemas/name2epsg.yaml))

To determine the format of the files, use the schemas referenced above, or
look at the [configuration directory that's used for the Unit-tests](tests/data/configs/)
which contains samples of all these files, except sources.yaml.

### 2.3 Common Tasks
The paragraphs below describe how to perform common tasks using the scripts in
this repository. Each script has a `--help` command line parameter to find out
all possible options.

#### 2.3.1 Harvesting Data
The following scripts are used to harvest data:

* [harvest_data.py](scripts/harvest_data.py) is used to harvest data from all
  sources in sources.yaml. This runs the harvesting asynchronously with up to
  6 portals being harvested simultaneously.
* [harvest_single_portal.py](scripts/harvest_single_portal.py) is used to
  harvest data from a single portal that is in sources.yaml
* [harvest_single_portal_debug.py](scripts/harvest_single_portal_debug.py) is
  used to test harvesting sources that are not yet in sources.yaml

#### 2.3.2 Analyzing Harvested Data
Use [analyze_data.py](scripts/analyze_data.py) to analyze harvested data, for
example to determine what fields are in the data, and get an overview of the
different types of data these fields contain. These scripts can be used to
determine the optimal data translation settings, and to develop new data
structurers and translators for new data sources.

#### 2.3.3 Processing Data
The following scripts are used to process harvested data. During processing,
the harvested data is (1) structured, and the structured data is (2)
translated into the correct format for OpenDaL.

* [process_data_multicore.py](scripts/process_data_multicore.py) is used to
  process a folder of harvested data harvested using the 'harvest_data.py'
  script. It uses multiprocessing to speed-up the processing
* [process_single_portal.py](scripts/process_single_portal.py) does the same as
  above, but for just one harvested data file
* [structure_single_portal.py](scripts/structure_single_portal.py) performs
  only the structuring step on the data of a single portal, but does not yet
  translate the structured data

#### 2.3.4 Others
* [generate_data_stats.py](scripts/generate_data_stats.py) is used to generate
  a 'stats.json' for a folder of harvested or structured data. This records the
  file-size and number of items for each portal.
* [compare_data_stats.py](scripts/compare_data_stats.py) is used to compare the
  stats.json files between two directories. This script will log any
  significant changes to the console, for example to investigate the difference
  between two harvests, so any failures of specific APIs can be spotted
* [CSW_list_capabilities.py](scripts/CSW_list_capabilities.py) is used to query
  a CSW to get the supported data formats to be used for queries. Do this if
  you want to set-up a CSW harvesting source, to determine the correct
  harvesting settings
* [upload_to_ES.py](scripts/upload_to_ES.py) is used to push a folder of
  structured data to ElasticSearch
* [delete_es_index.py](scripts/delete_es_index.py) is used to delete an ES
  index, so new data can be pushed to the ES instance

## 3. Development
To further develop and update the software in this repository, please follow
the installation steps in section 2. Also enable the githooks to execute tests
on commit:
```bash
./enable_githooks.sh
```

During development, please update the tests for the various parts of the
package. It is required that all preparsers, translators and structurers are
covered by the tests. The test will check this, and fail if this is not the
case.