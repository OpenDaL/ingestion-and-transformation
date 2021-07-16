# -*- coding: utf-8 -*-
"""
Contains the tests for the structurers
"""
from metadata_ingestion import structurers, resource
from helpers import load_data, compare_output

testdata = load_data('structurers.yaml')


def test_all_structurers_covered():
    """
    Test if the testdata covers all structurers
    """
    all_structurers = {
        o for o in dir(structurers) if o.endswith('Structurer')
        and o != 'Structurer'
    }

    assert set(testdata.keys()) == all_structurers,\
        "Not all structurers covered by tests"


def test_structurers():
    """
    In/output tests of the structurers
    """
    for structurer_name, tests in testdata.items():
        StructurerClass = getattr(structurers, structurer_name)
        for test in tests:
            structurer = StructurerClass(*test['args'], **test['kwargs'])
            metadata = resource.ResourceMetadata(test['input'])
            structurer.structure(metadata)
            compare_output(
                metadata.structured,
                test['output']['structured'],
                all_fields=True,
                assert_none=False
            )
            compare_output(metadata.meta, test['output']['meta'])
