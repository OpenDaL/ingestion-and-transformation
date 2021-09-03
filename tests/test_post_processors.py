# -*- coding: utf-8 -*-
"""
Contains the tests for the structurers
"""
from pathlib import Path
from helpers import load_data, compare_output
# Override config path so it's using the test specific configs
config_path = Path(
    Path(__file__).absolute().parent,
    'configs'
)
from metadata_ingestion import settings
settings.INGESTION_CONF_DIR = config_path
from metadata_ingestion import resource
from metadata_ingestion.post_processors import Filter, Optimizer


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
