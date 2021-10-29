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
from metadata_ingestion import translators, resource  # noqa: E402


testdata = load_data('translators.yaml')


def test_all_translators_covered():
    """
    Test if the testdata covers all structurers
    """
    all_translators = {
        o for o in dir(translators) if o.endswith('Translator')
        and o != 'FieldTranslator' and o != 'MetadataTranslator'
        and o != 'DateTranslator' and o != 'MetaTranslator'
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
                metadata = resource.ResourceMetadata({})
                metadata.structured = test['_structured']
                translator.translate(metadata)
                compare_output(
                    metadata.translated, test['_translated'], all_fields=True)
