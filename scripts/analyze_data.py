# -*- coding: utf-8 -*-
"""
Create analysis results for each file in the input directory

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
import logging
import argparse
from pathlib import Path

from metadata_ingestion import analyze, dataio


def is_valid_folder(parser: argparse.ArgumentParser, dirloc: str):
    """
    Checks if the provided dicloc is valid
    """
    path = Path(dirloc)
    if not path.is_dir():
        parser.error('The directory {} does not exist'.format(dirloc))
    else:
        return path


if __name__ == '__main__':
    # Parse the script arguments
    aparser = argparse.ArgumentParser(
        description=(
            "Analyzed Harvested or Processed data files to create a summary of"
            " the data in these files"
        )
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
    aparser.add_argument(
        "--structure",
        help=(
            "If provided, the data is structured first. This requires the ",
            "portal to be in sources.yaml, so the structurer configuration can"
            " be read."
        ),
        action='store_true'
    )
    aparser.add_argument(
        "--merge-results",
        help=(
            "Merge the results from the multiple files into one combined file"
        ),
        action='store_true'
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
        analyze.single_file(
            file_loc, structure_data=args.structure, out_folder=out_folder
        )
        logger.info('Processed {} of {}'.format(ind_ + 1, nr_of_files))

    if args.merge_results:
        logger.info('Merging results...')
        analyze.merge_to_examples(
            out_folder,
            Path(out_folder, 'COMBINED_analysis.json')
        )

    logger.info('Done')
