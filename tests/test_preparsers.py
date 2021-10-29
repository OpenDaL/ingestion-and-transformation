# -*- coding: utf-8 -*-
"""
Contains the tests for the pre-parsers

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
from metadata_ingestion import translators, resource
from helpers import load_data, compare_output

testdata = load_data('preparsers.yaml')


def test_all_preparsers_covered():
    """
    Test if the testdata covers all preparsers
    """
    all_preparsers = {
        o for o in dir(translators) if o.endswith('Preparser')
        and o != 'Preparser'
    }

    assert set(testdata.keys()) == all_preparsers,\
        "Not all preparsers covered by tests"


def test_preparsers():
    """
    In/output tests of the preparsers
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
