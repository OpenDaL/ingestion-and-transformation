# -*- coding: utf-8 -*-
"""
Test script that performs several tests on the translation process. Running
the `pytest` command, will execute all functions that start with 'test'

_Note: Currently this lives alongside the test_post_processing.py, with
specific tests for post_processing. Purpose is to move new tests here, and
add them in the same way as test_translation_and_post_processing(), so there's
an easy YAML file where you can specify expected input and output of the tests_
"""
from metadata_ingestion import translate, post_process
from helpers import load_data, compare_output

default_external_reference = {
    'URL': 'http://test.test.test',
    'type': 'synchronizedPortalPage'
}


def fill_missing_translation_input(entry):
    """
    Fill missing fields in translation input, so translation doesn't fail
    """
    entry.setdefault('_dplatform_uid', 'testid')
    entry.setdefault('_dplatform_externalReference',
                     default_external_reference)


# TESTS
# All test functions should start with 'test_', so that they're found by pytest
def test_translation_and_post_processing():
    """
    Test the translation and post_processing steps, using a YAML file with
    expected in/output
    """
    test_data = load_data('translation_and_post_processing.yaml')

    for i, d in enumerate(test_data):
        input = d['input']
        fill_missing_translation_input(input)

        # Do translation and post_processing steps
        translated_entry = translate.single_entry(input, 'testpid')

        if post_process.is_filtered(translated_entry):
            assert 'output' not in d, "Output specified, but entry is filtered"
            continue
        else:
            assert 'output' in d, "Item not filtered, but no output specified"
            post_process.optimize(translated_entry)
            post_process.score(translated_entry)

        # Now check agains output
        compare_output(translated_entry, d['output'])

