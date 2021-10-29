# -*- coding: utf-8 -*-
"""
Contains the tests for the structurers

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
from helpers import load_data, compare_output
# Override config path so it's using the test specific configs
config_path = Path(
    Path(__file__).absolute().parent,
    'configs'
)
from metadata_ingestion import settings  # noqa: E402
settings.INGESTION_CONF_DIR = config_path
from metadata_ingestion import resource  # noqa: E402
from metadata_ingestion.post_processors import Filter, Optimizer  # noqa: E402


testdata = load_data('post_processors.yaml')


def test_filter():
    """
    Test the Filter Post Processor
    """
    filter_pp = Filter()
    for testcase in testdata['Filter']:
        metadata = resource.ResourceMetadata({})
        metadata.translated = testcase['translated']
        filter_pp.post_process(metadata)
        assert metadata.is_filtered == testcase['is_filtered']


def test_optimizer():
    """
    Test the Optimizer Post Processor
    """
    optimizer_pp = Optimizer()
    for testcase in testdata['Optimizer']:
        metadata = resource.ResourceMetadata({})
        metadata.translated = testcase['translated']
        optimizer_pp.post_process(metadata)
        compare_output(metadata.translated, testcase['optimized'])
