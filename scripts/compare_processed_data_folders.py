# -*- coding: utf8 -*-
"""
Script to compare two folders that contain processed data, to check if they
contain the same data
"""
from pathlib import Path

from metadata_ingestion import dataio


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
    difference_message = '\nExpected: {}\n\nActual:{}'.format(
        str(reference), str(actual)
    )
    if all_fields:
        assert len(actual) == len(reference), \
            f"Field length difference found:\n{difference_message}"
        if actual == reference:  # If quick test fails, below logic is needed
            return

    for key, reference_value in reference.items():
        if reference_value is not None or not assert_none:
            assert key in actual, (
                "Key {} not in actual".format(key)
                + difference_message
            )
            actual_value = actual[key]
        else:
            assert key not in actual, (
                "Key {} should not be in actual".format(key)
                + difference_message
            )
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
                                compare_output(
                                    actual_item,
                                    ref_item,
                                    all_fields=all_fields,
                                    assert_none=assert_none
                                )
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
            compare_output(
                actual_value,
                reference_value,
                all_fields=all_fields,
                assert_none=assert_none
            )
        else:
            assert actual_value == reference_value


def compare(id_, original, reference):
    try:
        compare_output(original, reference, assert_none=False, all_fields=True)
    except AssertionError:
        print(f"Data for id '{id_}' is not equal")
        raise


if __name__ == '__main__':
    folder1 = Path('/home/brouwer/Downloads/opendal_data/original/')
    folder2 = Path('/home/brouwer/Downloads/opendal_data/reference/')

    # Rename fields in the original
    renamefields = {
        'abstractORdescription': 'description'
    }

    # List jsonl files in folder
    files_folder1 = [p for p in folder1.iterdir() if p.suffix == '.jsonl']
    files_folder2 = [p for p in folder2.iterdir() if p.suffix == '.jsonl']

    # Check for differences
    filesnames_folder1 = set([p.name for p in files_folder1])
    filesnames_folder2 = set([p.name for p in files_folder2])

    # Check for differences in filenames
    if not_in_2 := filesnames_folder1.difference(filesnames_folder2):
        raise ValueError(
            'The following files are in folder 1 but not in folder'
            f' 2: {not_in_2}'
        )

    if not_in_1 := filesnames_folder2.difference(filesnames_folder1):
        raise ValueError(
            'The following files are in folder 2 but not in folder'
            f' 1: {not_in_1}'
        )

    for f1_path in files_folder1:
        f2_path = Path(folder2, f1_path.name)

        # Caching is required, since data is processed async, so may be written
        # in a different order
        cached_original_ids = set()
        cached_originals = {}
        cached_reference_ids = set()
        cached_reference = {}
        checked_ids = set()
        count = 0
        print(f'Start checking file {f1_path.name}')
        for d_org, d_ref in zip(
                dataio.iterate_jsonlines(f1_path),
                dataio.iterate_jsonlines(f2_path)
                ):
            count += 1
            id_org = d_org['id']
            id_ref = d_ref['id']

            for org_field, new_field in renamefields.items():
                if org_field in d_org:
                    d_org[new_field] = d_org.pop(org_field)

            # If they're the same, direct compare
            if id_org == id_ref:
                compare(id_org, d_org, d_ref)
                continue

            # If they're not the same, look both up in other, if found check,
            # if not found, cache
            if id_org not in checked_ids:
                if id_org in cached_reference_ids:
                    cached_ref = cached_reference.pop(id_org)
                    cached_reference_ids.remove(id_org)
                    compare(id_org, d_org, cached_ref)
                    checked_ids.add(id_org)
                else:
                    cached_original_ids.add(id_org)
                    cached_originals[id_org] = d_org

            if id_ref not in checked_ids:
                if id_ref in cached_original_ids:
                    cached_org = cached_originals.pop(id_ref)
                    cached_original_ids.remove(id_ref)
                    compare(id_ref, cached_org, d_ref)
                    checked_ids.add(id_ref)
                else:
                    cached_reference_ids.add(id_ref)
                    cached_reference[id_ref] = d_ref

            if count % 5000 == 0:
                total_cached =\
                    len(cached_reference_ids) + len(cached_original_ids)
                assert total_cached < 50000, "Error: More than 50000 cached..."

                print(f'Processed {count} entries')

        if cached_original_ids or cached_reference_ids:
            raise ValueError('There are unmatched entries left in cache')

    print('Checking done!')
