# -*- coding: utf-8 -*-
"""
Only to be used as module. Functions shared across tests
"""

from pathlib import Path
import yaml

data_dir = Path(Path(__file__).parent, 'data')


def load_data(filename):
    """
    Load YAML test data with given filename
    """
    with open(Path(data_dir, filename), 'r', encoding="utf8") as yamlfile:
        return yaml.safe_load(yamlfile)


def compare_output(
        actual: dict, reference: dict, all_fields: bool = False,
        assert_none: bool = True):
    """
    Compare actual output of algorithms to a reference output.

    Arguments:
        actual -- The actual dict/list output

        reference -- The desired/reference output

        all_fields=False -- If True, an error is generated if
        the reference does not have all of the fields of the actual. Otherwise,
        only the fields that are in the reference, are compared to the actual.

        assert_none=True -- If True, fields in the reference with a null value
        should not be in the actual

    _Note: For lists it checks the length, whether an entry is included, but
    not order_
    """
    if all_fields:
        assert len(actual) == len(reference)
        if actual == reference:  # If quick test failse, below logic is needed
            return

    for key, reference_value in reference.items():
        if reference_value is not None or not assert_none:
            assert key in actual
            actual_value = actual[key]
        else:
            assert key not in actual
            continue

        assert type(reference_value) == type(actual_value)
        if isinstance(reference_value, list):
            # List lengths should be equal
            assert len(reference_value) == len(actual_value)
            for ref_item in reference_value:
                if ref_item not in actual_value:
                    # It can be that lists inside the dict are in a different
                    # order. Therefore, recheck by investigating each item
                    # seperately
                    if isinstance(ref_item, dict):
                        for actual_item in actual_value:
                            try:
                                compare_output(actual_item, ref_item)
                                break
                            except AssertionError:
                                continue
                        else:
                            # Already tested with if, but this displays info
                            assert ref_item in actual_value
                    else:
                        # Already tested with if, but this displays info
                        assert ref_item in actual_value

        elif isinstance(reference_value, dict):
            compare_output(actual_value, reference_value)
        else:
            assert actual_value == reference_value
