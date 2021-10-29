#!/bin/bash
SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

rm -r -f $SCRIPTPATH/.env
python3.9 -m venv $SCRIPTPATH/.env
source $SCRIPTPATH/.env/bin/activate
pip install wheel
pip install ./packages/metadata_ingestion
pip install pytest
