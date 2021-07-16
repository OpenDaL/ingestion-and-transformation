# -*- coding: utf-8 -*-
"""
Create analysis results for each file in the input directory
"""
import logging
import argparse
from pathlib import Path

from metadata_ingestion import analyze, dataio


def is_valid_folder(parser, dirloc):
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


def analyze_file(fileloc):
    analyze.single_file(fileloc, structure_data=True,
                        out_folder=out_folder)


if __name__ == '__main__':
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description="Analyzed Harvested or Processed data files"
    )
    aparser.add_argument(
        "in_folder",
        help="The folder that contains the files that should be analyzed",
        type=lambda x: is_valid_folder(aparser, x)
    )
    aparser.add_argument(
        "out_folder",
        help="The folder to store the analysis results",
        type=lambda x: is_valid_folder(aparser, x)
    )

    # Get arguments
    args = aparser.parse_args()
    in_folder = args.in_folder
    out_folder = args.out_folder

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    all_files = sorted(dataio.list_files(in_folder, 'jsonl'))
    nr_of_files = len(all_files)
    for ind_, file_loc in enumerate(all_files):
        analyze_file(file_loc)
        logger.info('Processed {} of {}'.format(ind_ + 1, nr_of_files))
