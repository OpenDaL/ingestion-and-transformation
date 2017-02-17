# Metadata Ingestion and Translation
This repository contains the python package and scripts related to the ingestion of metadata. The code in this repository is still in development and therefore a WIP. All code in this repository has been developed on _Python 3.9_

## Contents
The root directory of the repository contains two directories:

* __packages__: Contains installable python packages:
    1. _metadata\_ingestion_ - Package for ingesting metdata from several open data APIs
* __scripts__: Contains scripts that utilize the packages of the repository

Each package in this repository contains its own readme file, and can be installed by running `python setup.py install` in the top level directory of the package.

## Regular Tasks

### Filling the ES database
After harvested data has been processed, the processed data can be pushed to an
ES Server. The script is configured to push data to a ES 7.X instance. Make
sure that there is not pre-existing index called 'resource_metadata' on this
instance.

To push data to an ES instance:

1. Activate (or create and activate, if it's the first time) a python virtual
environment in terminal that has python 3.7.x and the 'requests' package
installed.
2. `cd` to the scripts directory and run `python ./upload_to_ES.py`
3. Provide the IP Address of the ES database (Public IP from EC2 Console).
Don't provide a port, the script automatically assumes 9200.
4. Provide the password for the 'data_upload' account on the ES Database
server. This can be found in the file `/home/ubuntu/es_creds.txt` on that
server
5. Provide the directory with the input (processed json-lines files).
6. The script will print progress messages.

The process of uploading data can
take up to twelve hours.