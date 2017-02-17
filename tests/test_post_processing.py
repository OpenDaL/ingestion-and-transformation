# -*- coding: utf-8 -*-
from os import path
import json

from metadata_ingestion import post_process

cur_dir = path.dirname(path.realpath(__file__))
with open(path.join(cur_dir, 'post_processing.json'), 'r', encoding='utf8') as\
        jsonfile:
    payload = json.load(jsonfile)


def _sort_dict(dict_):
    return dict(sorted(dict_.items()))


def test_filters():
    """
    Test whether the filters are working
    """
    remove_inds = payload['filters']['filter_inds']
    for ind_, entry in enumerate(payload['filters']['input']):
        if post_process.is_filtered(entry):
            assert ind_ in remove_inds, "Entry {} should not be filtered".format(
                str(entry)
            )
        else:
            assert ind_ not in remove_inds, "Entry {} should not be kept".format(
                str(entry)
            )


def test_optimizations():
    """
    Test whether the optimizations are working
    """
    pairs = payload['optimizations']['pairs']
    for input, output in pairs:
        post_process.optimize(input)
        sorted_output = _sort_dict(input)
        sorted_expectation = _sort_dict(output)
        assert sorted_output == sorted_expectation,\
            "Output {} does not match expectation {}".format(
                str(sorted_output),
                str(sorted_expectation)
            )
