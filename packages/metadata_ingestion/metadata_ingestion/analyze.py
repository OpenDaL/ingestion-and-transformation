# -*- coding: utf-8 -*-
"""
Analyze Sub-Module

Contains functions to help analyze ingested data, for example to check which
fields are found, and what the range of data is that these fields contain

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
import re
import math
from typing import Any, Union

from metadata_ingestion import dataio, _loadcfg, structurers, resource

CALCULATE_LEN_FOR = set(['list', 'dict'])


def _init_analysis_results(source_id: str = None) -> dict:
    """
    Creates the empty analysis results dictionary

    Args:
        source_id: The id of the platform to create a results dictionary for
    """
    if source_id is not None:
        analysis_results = {
            "full_entries": {
                source_id: {}
            },
            "metadata": {}
        }
    else:
        analysis_results = {
            "full_entries": {},
            "metadata": {}
        }

    return analysis_results


def _init_dtype_data(
        dtype_name: str, value: Any, full_entry_ref: dict
        ) -> dict:
    """
    Initializes the the result data for a specific data type

    Args:
        dtype_name:
            The data type name, e.g. 'str', 'dict' etc.
        value:
            The initial value that was found is added as both extremes (min
            size & max size) until more data is analyzed
        full_entry_ref:
            A reference to the entry where the value was taken from, with the
            source id as a key, and the index of the entry as a value

    Returns:
        Initial dictionary with data for the given data type for a field
    """
    samples_keyname = '{}_samples'.format(dtype_name)
    strfd_len = len(str(value))

    dtype_data = {
        samples_keyname: {
            "strfd_fe_refs_minmax": [full_entry_ref, full_entry_ref],
            "strfd_lenghts_minmax": [strfd_len, strfd_len],
            "strfd_samples_minmax": [value, value]
        }
    }

    if dtype_name in CALCULATE_LEN_FOR:
        length = len(value)
        dtype_data[samples_keyname].update({
            "fe_refs_minmax": [full_entry_ref, full_entry_ref],
            "lenghts_minmax": [length, length],
            "samples_minmax": [value, value],
        })

    return dtype_data


def _init_key_data(dtype_name, value, full_entry_ref):
    """
    Initializes the count and samples for a new key

    Args:
        dtype_name:
            The data type name, e.g. 'str', 'dict' etc.
        value:
            The initial value that was found is added as both extremes (min
            size & max size) until more data is analyzed
        full_entry_ref:
            A reference to the entry where the value was taken from, with the
            source id as a key, and the index of the entry as a value

    Returns:
        Initial dictionary with data for the given data type for a field and
        count set to 1
    """

    key_data = {'count': 1}

    key_data.update(_init_dtype_data(dtype_name, value, full_entry_ref))

    return key_data


def _update_dtype_data(
        dtype_data: dict, dtype_name: str, value: Any, full_entry_ref: dict
        ) -> bool:
    """
    Update the samples for the dtype, in case the value is shorter or longer
    than previously scanned values. Returns 'True' in case something is updated
    or 'False' in case nothing has changed.

    Args:
        dtype_data:
            The data to update (initialized using the _init_dtype_data
            function)
        dtype_name:
            The data type name, e.g. 'str', 'dict' etc.
        value:
            The value to update the data with
        full_entry_ref:
            A reference to the entry where the value was taken from, with the
            source id as a key, and the index of the entry as a value

    Returns:
        Whether the 'dtype_data' variable was updated or not
    """
    updated = False
    strfd_len = len(str(value))

    ind_ = 2
    if strfd_len < dtype_data["strfd_lenghts_minmax"][0]:
        ind_ = 0
    elif strfd_len > dtype_data["strfd_lenghts_minmax"][1]:
        ind_ = 1

    if ind_ < 2:
        dtype_data["strfd_lenghts_minmax"][ind_] = strfd_len
        dtype_data["strfd_fe_refs_minmax"][ind_] = full_entry_ref
        dtype_data["strfd_samples_minmax"][ind_] = value
        updated = True

    if dtype_name in CALCULATE_LEN_FOR:
        length = len(value)
        ind_ = 2
        if length < dtype_data["lenghts_minmax"][0]:
            ind_ = 0
        elif length > dtype_data["lenghts_minmax"][1]:
            ind_ = 1

        if ind_ < 2:
            dtype_data["lenghts_minmax"][ind_] = length
            dtype_data["fe_refs_minmax"][ind_] = full_entry_ref
            dtype_data["samples_minmax"][ind_] = value
            updated = True

    return updated


def _update_analysis_results(
        analysis_results: dict, key: str, value: Any, full_entry_ref: dict
        ) -> bool:
    """
    Update the analysis results, with the given key-value pair. In case
    something is updated, it returns 'True', else 'False'

    Args:
        analysis_results:
            The overall analysis results, updated in-place
        key:
            The key under which the value is found
        value:
            The data found under the given key
        full_entry_ref:
            A reference to the entry where the value was taken from, with the
            source id as a key, and the index of the entry as a value

    Returns:
        Whether the 'analysis_results' variable was updated or not
    """
    dtype_name = type(value).__name__
    samples_keyname = '{}_samples'.format(dtype_name)

    if key in analysis_results['metadata']:
        analysis_results['metadata'][key]['count'] += 1
        if samples_keyname in analysis_results['metadata'][key]:
            updated = _update_dtype_data(
                analysis_results['metadata'][key][samples_keyname],
                dtype_name,
                value,
                full_entry_ref
                )
        else:
            analysis_results['metadata'][key].update(
                _init_dtype_data(dtype_name, value, full_entry_ref)
                )
            updated = True
    else:
        analysis_results['metadata'][key] = _init_key_data(dtype_name,
                                                           value,
                                                           full_entry_ref)
        updated = True

    return updated


def _remove_count_below(analysis_results: dict, min_count: int):
    """
    Removes all keys from the analysis results, if they have a count lower than
    min_count

    Args:
        analysis_results:
            The overall analysis results data
        min_count:
            If keys lower than this count are in the analysis_results, their
            data is removed
    """
    remove_keys = [k for k, v in analysis_results['metadata'].items() if
                   v['count'] < min_count]

    for key in remove_keys:
        del analysis_results['metadata'][key]


def _clean_unreferenced_entries(analysis_results: dict):
    """
    Cleans unreferenced data under the 'full_entries' key

    Unreferenced entries, are entries that do not have any key samples under
    the 'metadata' section

    Args:
        analysis_results:
            The data to clean unreferenced entries from (Edited in-place)
    """
    # Collect references per portal:
    referenced = {}
    for k, v in analysis_results['metadata'].items():
        for sk, sv in v.items():
            if sk.endswith('_samples'):
                for ssk, ssv in sv.items():
                    if 'refs' in ssk:
                        for entry_ref in ssv:
                            source_id, ind_ = next(iter(entry_ref.items()))
                            if source_id not in referenced:
                                referenced[source_id] = set()
                            referenced[source_id].add(ind_)

    # Delete unreferenced portals:
    portals = set(analysis_results['full_entries'].keys())
    referenced_portals = set(referenced.keys())
    remove_portals = portals.difference(referenced_portals)
    for portal_id in remove_portals:
        del analysis_results['full_entries'][portal_id]

    # Delete unreferenced indices:
    for portal_id in analysis_results['full_entries']:
        indices = set(analysis_results['full_entries'][portal_id].keys())
        referenced_indices = referenced[portal_id]
        remove_indices = indices.difference(referenced_indices)
        for ind_ in remove_indices:
            del analysis_results['full_entries'][portal_id][ind_]


def single_file(
        in_fileloc: Union[str, Path], structure_data: bool = False,
        out_folder: Union[str, Path] = None, load_first: int = None,
        exclude_indices: list[int] = None, min_count: int = None
        ) -> Union[None, dict]:
    """
    Analyze the harvested raw data from in a single json-lines file

    Arguments:
        in_fileloc:
            The location of the input file. From this fileloc it tries to
            derive the platform id
        structure_data:
            Whether or not to structure the data before analysis
        out_folder:
            Output location. If defined, a json file with the analysis results
            is stored here
        load_first:
            When defined, only this many lines from the beginning of the files
            are analyzed
        exclude_indices:
            When defined, these indices will not be considered in the analysis
        min_count:
            When defined, keys that are in the data less than this, will be
            filtered from the output data

    Returns:
        (In case out_folder = None) The analysis results
    """
    filename = Path(in_fileloc).name

    # Get id from filename
    re_id = re.match(r'(^.*)_', filename)
    source_id = re_id.group(1)

    # Lookup config for id:
    sources = _loadcfg.sources()
    match = [s for s in sources if s['id'] == source_id]
    if match == 0:
        raise ValueError('Platform id not found in sources.json')
    sdata = match[0]

    analysis_results = _init_analysis_results(source_id)

    if structure_data:
        structurer = getattr(structurers, sdata['structurer'])(
            sdata['id'], **sdata['structurer_kwargs']
        )

    # Analyze key/value pairs
    for ind_, entry in enumerate(dataio.iterate_jsonlines(in_fileloc)):
        if load_first is not None and ind_ == load_first:
            break
        elif exclude_indices is not None and ind_ in exclude_indices:
            continue
        add_to_full_entries = False
        struc_entry = entry
        if structure_data:
            metadata = resource.ResourceMetadata(entry)
            structurer.structure(metadata)
            if not metadata.is_filtered:
                struc_entry = metadata.structured
            else:
                continue

        for k, v in struc_entry.items():
            entry_ref = {source_id: str(ind_)}
            updated = _update_analysis_results(analysis_results, k, v,
                                               entry_ref)
            if updated:
                add_to_full_entries = True

        if add_to_full_entries:
            analysis_results['full_entries'][source_id][str(ind_)] =\
                struc_entry

    if min_count is not None:
        _remove_count_below(analysis_results, min_count)

    # Cleanup entries no longer referenced:
    _clean_unreferenced_entries(analysis_results)

    # Write to folder, when requested
    if out_folder:
        out_fn = Path(in_fileloc).stem + '_analysis.json'
        out_fileloc = Path(out_folder, out_fn)
        dataio.savejson(analysis_results, out_fileloc)

    return analysis_results


def merge_to_examples(
        in_dir: Union[Path, str], out_fileloc: Union[Path, str] = None,
        min_count: int = None
        ) -> Union[None, dict]:
    """
    Merge all analysis result files in a directory, to one file containing all
    examples in the files

    Args:
        in_dir:
            The directory from which all .json files are used for merging
        out_fileloc:
            Optional; The path to save the merged data
        min_count:
            Optional; Do not include keys in the data that occur less than this
            amount in the combined data

    Returns:
        (If no output_loc is specified) The merged data
    """
    merged_data = _init_analysis_results()

    in_dir = Path(in_dir)

    all_json_files = [fp for fp in in_dir.iterdir() if fp.suffix == '.json']

    for fileloc in all_json_files:
        analysis_data = dataio.loadjson(fileloc)

        merged_data['full_entries'].update(analysis_data['full_entries'])

        for key, value in analysis_data['metadata'].items():
            dtype_samples = [skey for skey in value.keys() if
                             skey.endswith('samples')]
            if key in merged_data['metadata']:
                merged_data['metadata'][key]['count'] += value['count']
            else:
                merged_data['metadata'][key] = {
                    'count': value['count']
                }
            for skey in dtype_samples:
                svalue = value[skey]
                all_refs = svalue['strfd_fe_refs_minmax']
                all_samples = svalue['strfd_samples_minmax']

                all_refs.extend(svalue.get('fe_refs_minmax', []))
                all_samples.extend(svalue.get('samples_minmax', []))

                filt_all_refs = []
                filt_all_samples = []
                for ind_, ref in enumerate(all_refs):
                    if ref not in filt_all_refs:
                        filt_all_refs.append(ref)
                        filt_all_samples.append(all_samples[ind_])

                if skey in merged_data['metadata'][key]:
                    merged_data['metadata'][key][skey]['refs'].extend(
                        filt_all_refs
                    )
                    merged_data['metadata'][key][skey]['samples'].extend(
                        filt_all_samples
                    )
                else:
                    merged_data['metadata'][key][skey] = {
                        'refs': filt_all_refs,
                        'samples': filt_all_samples
                    }

    if min_count is not None:
        _remove_count_below(merged_data, min_count)

    # Cleanup entries no longer referenced:
    _clean_unreferenced_entries(merged_data)

    if out_fileloc is not None:
        dataio.savejson(merged_data, out_fileloc)

    return merged_data


def _preferred_to_new_inds(preferred_indices: list[int]) -> list[int]:
    """
    Uses a list of preferred indices, and investigates which index each one
    should really get in a list that is 1 smaller. Exports the index of the old
    list for each spot in the new list (that is one smaller).
    """
    new_inds = []
    one_skipped = False
    for i in range(len(preferred_indices) - 1):
        try:
            relind = preferred_indices[i:(i+2)].index(i)
        except ValueError:
            absdifs = [abs(i - p) for p in preferred_indices[i:(i+2)]]
            relind = absdifs.index(min(absdifs))

        keepind = relind + i
        new_inds.append(keepind)
        if keepind > i:
            one_skipped = True
            break

    if one_skipped:
        new_inds.extend(list(range(i + 2, len(preferred_indices))))

    return new_inds


def data_samples_for_key(
        in_fileloc: Union[Path, str], keyname: str, samples: int = 10
        ) -> tuple[list[dict], list[int], list[dict]]:
    """
    Generate examples for a key, from a json-lines file

    Args:
        in_fileloc:
            The location of the input json-lines file
        keyname:
            The key to investigate
        samples:
            The number of samples requested

    Returns:
        (1) List of data samples, (2) List with the indices of the given
        samples and (3) the full-entries from which the given samples were
        extracted
    """
    data_samples = []
    data_inds = []
    full_entries = []
    for ind_, dat in enumerate(dataio.iterate_jsonlines(in_fileloc)):
        struc_entry = dat
        keydata = struc_entry.get(keyname)
        if keydata is not None:
            if len(data_samples) < samples:
                data_samples.append(keydata)
                data_inds.append(ind_)
                full_entries.append(struc_entry)
            else:
                # Check prefered inidices of current stack:
                prefr_sample_indices = [math.ceil(((oind_ + 1) / (ind_ + 1))
                                        * samples) - 1 for oind_ in data_inds]

                # If none of the previous samples is allocated to the last spot
                # insert a new one, and reorder
                if prefr_sample_indices[-1] != (samples-1):
                    keep_indices = _preferred_to_new_inds(prefr_sample_indices)

                    data_samples = [data_samples[i] for i in keep_indices]
                    data_inds = [data_inds[i] for i in keep_indices]
                    full_entries = [full_entries[i] for i in keep_indices]

                    data_samples.append(keydata)
                    data_inds.append(ind_)
                    full_entries.append(struc_entry)

    return data_samples, data_inds, full_entries


def analysis_file_key_samples(
        in_fileloc: Union[Path, str], keyname: str
        ) -> tuple[list[dict], list[int], list[dict]]:
    """
    Generate examples for a original key from structured full_entries in an
    analysis file.

    Args:
        in_fileloc:
            The location of the json file with analysis results
        keyname:
            The key to investigate

    Returns:
        (1) List of data samples, (2) List with the indices of the given
        samples and (3) the full-entries from which the given samples were
        extracted
    """
    data_samples = []
    full_entries = []

    analysis_data = dataio.loadjson(in_fileloc)

    for portal, entries in analysis_data['full_entries'].items():
        for ind_, struc_entry in entries.items():
            if keyname in struc_entry:
                data_samples.append(struc_entry[keyname])
                struc_entry['_dportal_id_info'] = {portal: ind_}
                full_entries.append(struc_entry)

    return data_samples, full_entries
