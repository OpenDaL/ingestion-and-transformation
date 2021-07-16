# -*- coding: utf-8 -*-
"""
Contains the tests for the structurers
"""
from metadata_ingestion import translators, resource
from helpers import load_data, compare_output

testdata = load_data('preparsers.yaml')


def test_all_preparsers_covered():
    """
    Test if the testdata covers all structurers
    """
    all_preparsers = {
        o for o in dir(translators) if o.endswith('Preparser')
        and o != 'Preparser'
    }

    assert set(testdata.keys()) == all_preparsers,\
        "Not all preparsers covered by tests"


def test_preparsers():
    """
    In/output tests of the structurers
    """
    for preparser_name, testcases in testdata.items():
        PreparserClass = getattr(translators, preparser_name)
        for case in testcases:
            preparser = PreparserClass(**case['kwargs'])
            for test in case['preparse_function_tests']:
                metadata = resource.ResourceMetadata({})
                metadata.structured = test['_structured_before']
                return_data = preparser.preparse(metadata)
                compare_output(metadata.structured, test['_structured_after'])
                compare_output(return_data, test['_return'], all_fields=True)
