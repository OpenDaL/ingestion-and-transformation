# -*- coding: utf-8 -*-
"""
AUXILIARY FUNCTIONS MODULE

Contains general data processing functions (e.g. XML processing, dict cleanup,
string conversion etc.)
"""
import html
import re
import json
import ast
from typing import Union, Any

from metadata_ingestion.settings import REP_TEXTKEY

at_pattern = re.compile('@')


def get_first_key_data_with_len(dict_, len_: int) -> Any:
    """
    Gets the data of the first key from a dict with two letter language keys
    """
    for key in dict_.keys():
        if len(key) == len_:
            return dict_[key]
    else:
        return None


def get_data_from_loc(
        data: Union[dict, list], loc: Union[str, dict],
        pop: bool = False, default: Any = None, accept_subvalue: bool = False
        ) -> Any:
    """
    Gets the data from a specific location inside a dictionary

    Args:
        data:
            The input dict/list
        loc:
            The location/nested location of the resource description list
            (e.g. {'RDF': {'Catalog': 'dataset'}}, gets it from
            data['RDF']['Catalog']['dataset'])
        pop:
            Whether to use pop or get on the final level
        default:
            Default value to return, if the result was not found
        accept_subvalue:
            If the complete chain defined under 'loc' is not present in the
            data, this decides if a sub-value should be returned. e.g. if
            loc={'parentkey': 'childkey'}, but data={'parentkey': 4}, the
            value for parentkey would be returned

    Returns:
        The data at the location, if found, otherwise the default value
    """
    if isinstance(loc, str):
        if isinstance(data, dict):
            if pop:
                return data.pop(loc, default)
            else:
                return data.get(loc, default)
        elif isinstance(data, list):
            if pop:
                loc_data = [it.pop(loc) for it in data if isinstance(it, dict)]
            else:
                loc_data = [it.get(loc) for it in data if isinstance(it, dict)]
            return [it for it in loc_data if it is not None]
        elif accept_subvalue:
            return data
        else:
            return default
    else:
        key = next(iter(loc.keys()))
        new_loc = loc[key]
        if isinstance(data, dict):
            new_data = data.get(key)
            if new_data is not None:
                return get_data_from_loc(
                    new_data, new_loc, pop=pop, default=default,
                    accept_subvalue=accept_subvalue
                )
            else:
                return default
        elif isinstance(data, list):
            new_data = []
            for item in data:
                if isinstance(item, dict):
                    new_dat = item.get(key)
                    if new_dat is not None:
                        result = get_data_from_loc(
                            new_dat, new_loc, pop=pop, default=default,
                            accept_subvalue=accept_subvalue
                        )
                        if result is not default:
                            new_data.append(result)
            return new_data if new_data != [] else default
        elif accept_subvalue:
            return data
        else:
            return default


def rename_if_duplicate(keyname: str, data: dict) -> str:
    """
    Renames a key if it is a duplicate

    Args:
        keyname:
            The original name of the key
        data:
            The dict to look for duplicates

    Returns:
        The keyname to be used. This is the same as the input keyname in case
        no duplicates are found
    """
    org_keyname = keyname
    counter = 1

    while keyname in data:
        keyname = '{}_{}'.format(org_keyname, counter)
        counter += 1

    return keyname


def string_conversion(str_) -> str:
    """
    Checks if a string represents a list or dict (e.g. "[1, 6]") and convert
    when possible

    Returns:
        If it can be converted, returns the parsed string, otherwise the
        original is returned
    """
    result = str_

    if len(str_) > 1 and str_[0] == '"' and str_[-1] == '"':
        try:
            result = json.loads(str_)
        except json.JSONDecodeError:
            pass

    if len(str_) > 1 and\
       ((str_[0] == '[' and str_[-1] == ']')
       or (str_[0] == '{' and str_[-1] == '}')):
        try:
            result = ast.literal_eval(str_)
            if isinstance(result, set):  # Set is not JSON serializable
                result = list(result)
        except (SyntaxError, ValueError):
            try:
                result = json.loads(str_)
            except json.JSONDecodeError:
                pass

    return result


def remove_empty_keys(dict_) -> dict:
    """
    Removes keys with emtpy values from a dict

    Returns:
        A new dictionary, where all keys with empty values are omitted
    """
    return {
        k: v for k, v in dict_.items() if
        v != ''
        and v != '[]'
        and v != '{}'
        and v != []
        and v != {}
        and v is not None
    }  # 0 and False should not be excluded


def multiple_rename(str_: str, renaming: list[tuple[re.Pattern, str]]) -> str:
    """
    Renames a string based on multiple regexes and replace strings

    Args:
        str_ :
            String to be renamed

        renaming:
            List of 'length 2' tuples, which contain (1) a regex pattern,
            and (2) a string to which to replace the pattern with

    Returns:
        The renamed string
    """
    for pattern, replace in renaming:
        str_ = pattern.sub(replace, str_)

    return str_


def rename_keys(
        data: Union[dict, list], renaming: list[tuple[re.Pattern, str]]
        ) -> Union[dict, list]:
    """
    Renames all keys and embedded keys in a dict.

    Warning: Does not check for duplicates. Duplicate keys and data will be
    rewritten!

    Args:
        data:
            The key value pairs
        renaming:
            Each tupple giving a regex pattern and what to replace it with

    Returns:
        Data with the renamed keys
    """
    new_data = None
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            v_new = rename_keys(v, renaming)
            k_prop = multiple_rename(k, renaming)
            k_new = rename_if_duplicate(k_prop, new_data)
            new_data[k_new] = v_new
    elif isinstance(data, list):
        new_data = []
        for item in data:
            new_item = rename_keys(item, renaming)
            new_data.append(new_item)
    else:
        new_data = data

    return new_data


def remove_keys(data: dict, regex: Union[str, re.Pattern]) -> dict:
    """
    Removes keys that match specific regex patterns from a dict, even
    if keys are nested or in nested lists. Case insensitive

    Args:
        data:
            The data to be cleaned
        regex:
            If a key matches this regex, it is removed

    Returns:
        The cleaned data
    """
    if isinstance(regex, str):
        regex = re.compile(regex, re.IGNORECASE)

    new_data = None
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            if not regex.search(k):
                v_new = remove_keys(v, regex)
                new_data[k] = v_new
        if len(new_data) == 1 and '#text' in new_data:
            # This means that only the #text key is left, so instead of a dict,
            # it can just get the #text value
            new_data = new_data['#text']
    elif isinstance(data, list):
        new_data = []
        for item in data:
            new_item = remove_keys(item, regex)
            new_data.append(new_item)
    else:
        new_data = data

    return new_data


namespace_prefix_pattern = re.compile(r'^(@)?(.*?:)')


def remove_xml_namespaces(data: Union[dict, list]) -> Union[dict, list]:
    """
    Deletes the namespace prefixes from the keys in xml data (e.g. 'rdf:RDF'
    becomes 'RDF'), and removes 'xmlns:' and 'xsi:schemaLocation' attributes.

    Args:
        data: The parsed XML data

    Returns:
        The initial data, without namespace prefixes
    """
    # Remove 'xmlns' attributes:
    without_xmlns = remove_keys(data, r'(^@xmlns)|(^@xsi:schemaLocation)')

    # Remove namespace prefixes:
    renaming = [(namespace_prefix_pattern, r'\1')]
    data_without_namespace_info = rename_keys(without_xmlns, renaming)

    return data_without_namespace_info


def remove_nonetypes(data: Union[dict, list]) -> Union[dict, list]:
    """
    Removes 'None' values (nested) in data (e.g. dictionaries)

    Args:
        data: The data from which to remove the nonetype values

    Returns:
        The cleaned data
    """
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            v_new = remove_nonetypes(v)
            if v_new is not None:
                new_data[k] = v_new

    elif isinstance(data, list):
        new_data = []
        for item in data:
            new_item = remove_nonetypes(item)
            if new_item is not None:
                new_data.append(new_item)
        if new_data == []:
            new_data = None

    else:
        new_data = data

    return new_data


def clean_xml(
        key: str, value: Any, prefer_upper: bool = False
        ) -> tuple[str, Any]:
    """
    Clean and filter parsed xml data

    Args:
        key:
            The key name of the metadata attribute
        value:
            The data under the metadata attribute. All NoneTypes in
            the values (both top level and nested) should be removed using
            'remove_nonetypes'.
        prefer_upper:
            If a metadata attribute contains a dict of lenght 1, this
            determines whether the lower name will be retained, or
            the upper name, when the level in between is removed.

    Returns:
        The cleaned key and value
    """
    new_key = None
    new_value = None

    if isinstance(value, dict):
        """[Case 1]If a dict only contains 1 key, this intermediate level is
        removed, and the value is returned, as a key either the original
        (upper) or new (lower) key is retained. In case there is only a
        _content key as a child, the parent key is retained with the value of
        _content.

        [Case 2]In case of a larger dict, each value in the dict is simply fed
        to this function again"""

        # Case 1
        if len(value) == 1:
            first_key = next(iter(value))
            first_value = next(iter(value.values()))

            if prefer_upper or first_key == REP_TEXTKEY:
                new_key, new_value = clean_xml(key, first_value,
                                               prefer_upper=prefer_upper)
            else:
                new_key, new_value = clean_xml(first_key, first_value,
                                               prefer_upper=prefer_upper)

        # Case 2
        else:
            new_key = key
            new_value = {}
            for u_k, u_v in value.items():
                n_pk, n_v = clean_xml(u_k, u_v, prefer_upper=prefer_upper)
                n_k = rename_if_duplicate(n_pk, new_value)
                new_value[n_k] = n_v

    elif isinstance(value, list):
        """Note: in parsed XML data, a list within a list is not possible! also
        lists are always larger than 1 element. Data types other than string,
        dict, list and Nonetype are not in parsed XML data. NoneType is removed
        prior to using this function. The order of the list is retained.

        [Case 1]if all datatypes are dict, all are of length one and the names
        of all keys are the same, a list with the values is generated, and the
        key name becomes the new key (prefer_upper=False) or it inherets the
        key name of the upper level (prefer_upper=True). If the al key names
        are _content, the upper level key is inherited by default

        [Case 1.2] If the values are different, a list of these values is
        returned

        [Case 1.1] If all values of these keys are equal, the single value is
        returned.

        [Case 2]if all datatypes are dict, all are of length one but the names
        of the keys differ, or the length of each dict differs, these are
        stored as-is, and the underlying values are further processed using
        this function.

        [Case 3]If all datatypes are equal and not dict OR if there are
        different datatypes but all are not dict, the list will remain the
        same. Only duplicates are removed

        [Case 4] if there is a mixture of datatypes, including dicts, the other
        data types are also converted to a dict, in which the key-name is
        _content
        """
        # Determine Data types
        unique_data_types = set([type(i).__name__ for i in value])
        sample_item = value[0]
        same_datatypes = True if len(unique_data_types) == 1 else False

        if same_datatypes:
            if isinstance(sample_item, dict):
                # Determine the length of the dicts:
                unique_lengths = set([len(i) for i in value])
                sample_length = unique_lengths.pop()
                all_single_key = True if not unique_lengths and\
                    sample_length == 1 else False

                # Determine if there is a single key name:
                unique_keys = set([k for dct in value for k in dct])
                if not unique_keys:
                    # Catch if there are only emtpy dicts in a list
                    return key, None
                sample_key = unique_keys.pop()
                all_keys_one_name = True if not unique_keys else False

                # Case 1
                if all_single_key and all_keys_one_name:
                    # Test if all keys are _content:
                    if sample_key == REP_TEXTKEY or prefer_upper:
                        new_key = key
                    else:
                        new_key = sample_key

                    # Case 1.1
                    new_value = []
                    for i in value:
                        payload = next(iter(i.values()))
                        dummy_key, v = clean_xml(sample_key, payload,
                                                 prefer_upper=prefer_upper)
                        # If all of them have an underlying dict with one
                        # key this can still result in a list with dicts that
                        # all have the same key
                        new_value.append(v)

                    # Case 1.2
                    unique_str_values = set([str(i) for i in new_value])
                    if len(unique_str_values) == 1:
                        new_value = new_value[0]

                # Case 2
                else:
                    new_value = []
                    new_key = key
                    for i in value:
                        dummy_key, v = clean_xml(
                            key, i, prefer_upper=prefer_upper
                        )
                        new_value.append(v)
            # Case 3
            else:
                # Both set and list are used, because checking for inclusion in
                # set is faster, but order has to be retained.
                new_value = []
                for i in value:
                    if i not in new_value:
                        new_value.append(i)
                if len(new_value) == 1:
                    new_value = new_value[0]
                new_key = key
        # Case 4
        elif 'dict' in unique_data_types:
            new_value = []
            new_key = key
            content_values = set()  # To avoid duplicates
            for i in value:
                if isinstance(i, str):
                    if i not in content_values:
                        content_values.add(i)
                        v = {REP_TEXTKEY: i}
                        new_value.append(v)
                else:
                    d_k, v = clean_xml(key, i)
                    if isinstance(v, str):  # To prevent removal of level
                        if d_k == REP_TEXTKEY and v in content_values:
                            if v in content_values:
                                continue
                            else:
                                content_values.add(v)
                        v = {d_k: v}
                    else:
                        content_value = v.get(REP_TEXTKEY)
                        if content_value:
                            content_values.add(content_value)
                    new_value.append(v)

    elif isinstance(value, str):
        new_key = key
        if '&#' in value:
            new_value = html.unescape(value)
        else:
            new_value = value

    return new_key, new_value


def limit_depth(dict_, limit: int) -> dict:
    """
    Limits the depth a parsed XML structure, since some APIs tend to return
    crap data (Edits the input dict_ in-place)

    Args:
        dict_:
            The data to limit the depth of
        limit:
            The number of levels allowed

    Returns:
        The original data, depth limited
    """
    level = 0
    evaluate = [dict_]
    while True:
        next_evaluate = []
        level += 1
        for item in evaluate:
            if isinstance(item, (dict, list)):
                if level > limit:
                    item.clear()
                if isinstance(item, dict):
                    next_evaluate.extend(item.values())
                elif isinstance(item, list):
                    next_evaluate.extend(item)
        if not next_evaluate:
            break
        else:
            evaluate = next_evaluate

    return dict_


text_pattern = re.compile('#text')


def clean_xml_metadata(data: dict, prefer_upper: bool = False) -> dict:
    """
    Cleans an xml resource description parsed using xmltodict

    Arguments:
        data:
            The parsed XML data, that is to be cleaned
        prefer_upper:
            If an intermediate level in the dict is removed, because it only
            contained a single key, should the entry get the name of the lower
            (removed) key (default) or get the name of the upper key.

    Returns:
        The cleaned parsed XML data
    """
    # First the #text key and remove attribute declarations:
    renaming = [(text_pattern, REP_TEXTKEY), (at_pattern, '')]
    data = rename_keys(data, renaming)

    # Remove empty keys from dicts and indices from lists
    data = remove_nonetypes(data)

    cleaned_data = {}
    for k, v in data.items():
        prop_key, new_value = clean_xml(k, v, prefer_upper=prefer_upper)
        new_key = rename_if_duplicate(prop_key, cleaned_data)
        cleaned_data[new_key] = new_value

    return cleaned_data
