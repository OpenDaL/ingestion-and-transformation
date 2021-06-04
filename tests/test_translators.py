# -*- coding: utf-8 -*-
"""
Contains the tests for the structurers
"""
from metadata_ingestion import translators, resource
from helpers import load_data, compare_output

testdata = load_data('translators.yaml')


def test_all_translators_covered():
    """
    Test if the testdata covers all structurers
    """
    all_translators = {
        o for o in dir(translators) if o.endswith('Translator')
        and o != 'FieldTranslator' and o != 'MetadataTranslator'
        and o != 'DateTranslator'
    }

    assert set(testdata.keys()) == all_translators,\
        "Not all translators covered by tests"


def test_translators():
    """
    In/output tests of the structurers
    """
    for translator_name, testcases in testdata.items():
        TranslatorClass = getattr(translators, translator_name)
        for case in testcases:
            translator = TranslatorClass(**case['kwargs'])
            for test in case['translate_function_tests']:
                metadata = resource.ResourceMetadata({}, '')
                metadata.structured = test['_structured']
                translator.translate(metadata)
                compare_output(
                    metadata.translated, test['_translated'], all_fields=True)