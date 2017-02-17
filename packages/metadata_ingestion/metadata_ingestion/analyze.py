# -*- coding: utf-8 -*-
"""
ANALYZE SUB-module

Contains functions to help analyze ingested metadata, to aid the setup of
metadata translation
"""
import os
import re
import math

from metadata_ingestion import _dataio, _loadcfg, structure

CALCULATE_LEN_FOR = set(['list', 'dict'])


def _init_analysis_results(platform_id=None):
    """
    Initialize the analysis results for a specific platform_id
    """
    if platform_id is not None:
        analysis_results = {
            "full_entries": {
                platform_id: {}
            },
            "metadata": {}
        }
    else:
        analysis_results = {
            "full_entries": {},
            "metadata": {}
        }

    return analysis_results


def _init_dtype_data(dtype_name, value, full_entry_ref):
    """
    Initialize the key-data for a specific data type
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
    Initialize the count and samples for a new key
    """

    key_data = {'count': 1}

    key_data.update(_init_dtype_data(dtype_name, value, full_entry_ref))

    return key_data


def _update_dtype_data(dtype_data, dtype_name, value, full_entry_ref):
    """
    Update the samples for the dtype, in case the value is shorter or longer
    than previously scanned values. Returns 'True' in case something is updated
    or 'False' in case nothing has changed
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


def _update_analysis_results(analysis_results, key, value, full_entry_ref):
    """
    Update the analysis results, with the given key-value pair. In case
    something is updated, it returns 'True', else 'False'
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


def _remove_count_below(analysis_results, min_count):
    """
    Removes all keys from the analysis results, if they have a count lower than
    min_count
    """
    remove_keys = [k for k, v in analysis_results['metadata'].items() if
                   v['count'] < min_count]

    for key in remove_keys:
        del analysis_results['metadata'][key]


def _clean_unreferenced_entries(analysis_results):
    """
    Clean unreferenced data under the 'full_entries' key
    """
    # Collect references per portal:
    referenced = {}
    for k, v in analysis_results['metadata'].items():
        for sk, sv in v.items():
            if sk.endswith('_samples'):
                for ssk, ssv in sv.items():
                    if 'refs' in ssk:
                        for entry_ref in ssv:
                            platform_id, ind_ = next(iter(entry_ref.items()))
                            if platform_id not in referenced:
                                referenced[platform_id] = set()
                            referenced[platform_id].add(ind_)

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


def single_file(in_fileloc, structure_data=False, out_folder=None,
                load_first=None, exclude_indices=None, min_count=None):
    """
    Analyze the harvested raw data from in a single json-lines file

    Arguments:
        in_fileloc --- str: The location of the input file. From this fileloc
        it tries to derive the platform id

        structure_data --- bool: Whether or not to structure the data before
        analysis

        out_folder=None --- str: Output location. If defined, a json file with
        the analysis results is stored here

        load_first=None --- int: When defined, only this many lines from the
        beginning of the files are analyzed

        exclude_indices=None --- list[int]: When defined, these indices will
        not be considered in the analysis

        min_count=None --- int: When defined, keys that are in the data less
        than this, will be filtered from the output data

    Returns:
        dict --- The analysis results

    Output:
        json file --- The analysis results are saved, in case 'in_folder' is
        defined
    """
    filename = os.path.basename(in_fileloc)

    # Get id from filename
    re_id = re.match(r'(^.*)_', filename)
    platform_id = re_id.group(1)

    # Lookup config for id:
    sources = _loadcfg.sources()
    match = [s for s in sources if s['id'] == platform_id]
    if match == 0:
        raise ValueError('Platform id not found in sources.json')
    sdata = match[0]

    analysis_results = _init_analysis_results(platform_id)

    # Analyze key/value pairs
    for ind_, entry in enumerate(_dataio.iterate_jsonlines(in_fileloc)):
        if load_first is not None and ind_ == load_first:
            break
        elif exclude_indices is not None and ind_ in exclude_indices:
            continue
        add_to_full_entries = False
        struc_entry = entry
        if structure_data is not None:
            struc_entry = structure.single_entry(entry,
                                                 sdata['data_format'],
                                                 **sdata['structurer_kwargs'])
        for k, v in struc_entry.items():
            entry_ref = {platform_id: str(ind_)}
            updated = _update_analysis_results(analysis_results, k, v,
                                               entry_ref)
            if updated:
                add_to_full_entries = True

        if add_to_full_entries:
            analysis_results['full_entries'][platform_id][str(ind_)] =\
                struc_entry

    if min_count is not None:
        _remove_count_below(analysis_results, min_count)

    # Cleanup entries no longer referenced:
    _clean_unreferenced_entries(analysis_results)

    # Write to folder, when requested
    if out_folder:
        out_fn = 'analysis_' + filename
        out_fileloc = os.path.join(out_folder, out_fn)
        _dataio.savejson(analysis_results, out_fileloc)

    return analysis_results


def merge_to_examples(in_dir, out_fileloc=None, min_count=None):
    """
    Merge all analysis result files in a directory, to one file containing all
    examples in the files
    """
    merged_data = _init_analysis_results()

    all_json_files = [os.path.join(in_dir, fn) for fn in os.listdir(in_dir) if
                      fn.endswith('.json')]

    for fileloc in all_json_files:
        analysis_data = _dataio.loadjson(fileloc)

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
        _dataio.savejson(merged_data, out_fileloc)

    return merged_data


def _preferred_to_new_inds(preferred_indices):
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


def data_samples_for_key(in_fileloc, keyname, data_format=None,
                         samples=10):
    """
    Generate examples for a key, from a json-lines file

    Returns:
        (key_samples, sample_indices, full_entries)
    """
    data_samples = []
    data_inds = []
    full_entries = []
    for ind_, dat in enumerate(_dataio.iterate_jsonlines(in_fileloc)):
        struc_entry = dat
        if data_format is not None:
            struc_entry = structure.single_entry(dat, data_format)
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


def load_keys_for_tkey(tkeyname):
    """
    Returns a list of key names for the given translated key (key in the
    new metadata system)
    """
    key_mapping = _loadcfg.translation()

    # Reverse the mapping
    keys = []
    for key, tkeys in key_mapping.items():
        if tkeyname in tkeys:
            keys.append(key)

    return keys


def single_file_tkey_samples(in_fileloc, tkeyname, data_format=None,
                             samples=10):
    """
    Generate examples for a translated key from a single file with harvested
    data. Uses the translation file to find input keys belonging to a specific
    output key, and gives samples of these relevant keys.

    Returns:
        (key_samples, sample_indices, full_entries)
    """
    keys = load_keys_for_tkey(tkeyname)
    data_samples = []
    data_inds = []
    full_entries = []
    for ind_, dat in enumerate(_dataio.iterate_jsonlines(in_fileloc)):
        struc_entry = dat
        if data_format is not None:
            struc_entry = structure.single_entry(dat, data_format)
        pload = {}
        for keyname in keys:
            keydata = struc_entry.get(keyname)
            if keydata is not None:
                pload[keyname] = keydata

        if pload != []:
            if len(data_samples) < samples:
                data_samples.append(pload)
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

                    data_samples.append(pload)
                    data_inds.append(ind_)
                    full_entries.append(struc_entry)

    return data_samples, full_entries


def analysis_file_tkey_samples(in_fileloc, tkeyname):
    """
    Generate examples for a translated key from structured full_entries in an
    analysis file. Uses the translation file to find input keys belonging to a
    specific output key, and gives samples of these relevant keys.

    Returns:
        (data_samples, full_entries)
    """
    keys = load_keys_for_tkey(tkeyname)
    data_samples = []
    full_entries = []

    analysis_data = _dataio.loadjson(in_fileloc)

    for portal, entries in analysis_data['full_entries'].items():
        for ind_, struc_entry in entries.items():
            pload = {}
            for keyname in keys:
                keydata = struc_entry.get(keyname)
                if keydata is not None:
                    pload[keyname] = keydata

            if pload != {}:
                data_samples.append(pload)
                struc_entry['_dportal_id_info'] = {portal: ind_}
                full_entries.append(struc_entry)

    return data_samples, full_entries


def analysis_file_key_samples(in_fileloc, keyname):
    """
    Generate examples for a original key from structured full_entries in an
    analysis file.

    Returns:
        (key_samples, sample_indices, full_entries)
    """
    data_samples = []
    full_entries = []

    analysis_data = _dataio.loadjson(in_fileloc)

    for portal, entries in analysis_data['full_entries'].items():
        for ind_, struc_entry in entries.items():
            if keyname in struc_entry:
                data_samples.append(struc_entry[keyname])
                struc_entry['_dportal_id_info'] = {portal: ind_}
                full_entries.append(struc_entry)

    return data_samples, full_entries
