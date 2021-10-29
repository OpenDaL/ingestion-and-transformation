# -*- coding: utf-8 -*-
"""
Structurers Module

The structurer classes in this module enable the structuring of data that is
harvested from the Open Data Portals. Each Structurer should inherit from the
base 'Structurer' class, and can use Mixins for shared functionality.

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
import re
import logging
from abc import ABC, abstractmethod
from typing import Union
from urllib.parse import quote_plus

from metadata_ingestion.resource import ResourceMetadata
from metadata_ingestion import _common, sources

from metadata_ingestion.settings import REP_TEXTKEY

logger = logging.getLogger(__name__)


class Structurer(ABC):
    """
    Structurer base class.

    Use this as the base class of every structurer, possibly combined with
    other mixins defined in this module.
    """
    def __init__(self, source_id: str):
        """
        Initializes the Structurer instance

        Args:
            source_id:
                The id of the source to structure
        """
        self.source_id = source_id

    def _fill_structured(self, metadata: ResourceMetadata):
        """
        Fill metadata.structured. By default, this copies the harvested
        data into structured
        """
        metadata.structured = metadata.harvested

    def structure(self, metadata: ResourceMetadata):
        """
        Flatten and structure harvested data to prepare it for translation

        Arguments:
            metadata:A ResourceMetadata object with harvested data
        """
        self._fill_structured(metadata)
        metadata.meta['source']['id'] = self.source_id
        if metadata.is_filtered:
            return
        self._process(metadata)

        if metadata.is_filtered:
            return
        else:
            metadata.structured = _common.remove_empty_keys(
                metadata.structured
            )

    @abstractmethod
    def _process(self, metadata: ResourceMetadata):
        """
        Function that processes the metadata. Functionality is filled in by
        subclasses and Mixin classes
        """
        pass


def get_structurer(source_id: str) -> Structurer:
    """
    Factory method to get a configured structurer for the a specific source
    id

    Arguments:
        source_id: The Id of a source configured in sources.yaml
    """
    source_data = sources[source_id]
    structurer_class_name = source_data['structurer']
    structurer_kwargs = source_data.get('structurer_kwargs', {})

    return globals()[structurer_class_name](source_id, **structurer_kwargs)


# MIXIN CLASSES: These add functionlity to a structurer
class KeyIdMixin:
    """Structurer Mixin to derive ID from a specific key"""
    def __init__(
            self, *args, id_key: Union[dict, str] = None,
            id_from_harvested: bool = False, **kwargs
            ):
        """
        Initializes the Mixin instance

        This Mixin adds the following arguments to an inheriting class:

        Args:
            id_key:
                Key where the id is stored (See loc parameter of
                _common.get_data_from_loc() for the different options)
            id_from_harvested:
                Optional; If True, the id is retrieved from metadata.harvested
                rather than metadata.structured
        """
        self.id_key = id_key
        self.id_from_harvested = id_from_harvested
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.id_key is not None:
            if self.id_from_harvested:
                basedata = metadata.harvested
            else:
                basedata = metadata.structured

            local_id = str(
                _common.get_data_from_loc(
                    basedata, self.id_key, pop=True
                )
            )
            metadata.meta['localId'] = local_id
        super()._process(metadata)


class KeyUrlMixin:
    """
    Structurer Mixin to derive the url of an entry from a specific key
    """
    def __init__(
            self, *args, url_key: Union[dict, str] = None,
            url_from_harvested: bool = False, **kwargs
            ):
        """
        Initialize the Mixin Instance

        Args:
            url_key:
                Optional; The key from which to derive the URL. If not
                given, this Mixin is not used to add the URL. See
                _common.get_data_from_loc for format.

            url_from_harvested:
                Optional; If True, get the url from metadata.harvested rather
                than metadata.structured
        """
        self.url_key = url_key
        self.url_from_harvested = url_from_harvested
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.url_key is not None:
            if self.url_from_harvested:
                basedata = metadata.harvested
            else:
                basedata = metadata.structured

            url_data = _common.get_data_from_loc(
                basedata, self.url_key, pop=True
            )
            if isinstance(url_data, list):
                for item in url_data:
                    if item.startswith('http'):
                        url_data = item
                        break

            metadata.meta['url'] = url_data
        super()._process(metadata)


class BaseUrlMixin:
    """
    Structurer Mixin to derive by appending the id, or value of another key to
    a provided base_url
    """
    def __init__(
            self, *args, base_url: str = None, url_suffix_key: str = None,
            url_suffix_from_harvested: bool = False,
            url_id_suffix_as_backup: bool = False,
            strip_base_url_slash: bool = False,
            base_url_suffix_mapping_key: Union[str, dict] = None,
            base_url_suffix_mapping: dict = None, **kwargs
            ):
        """
        Initializes the Mixin instance

        Args:
            base_url:
                Optional; The url to append the id or other key value to.
                If not given it is assumed another UrlMixin is used, and this
                code is not run
            url_suffix_key:
                If provided the content of this key is used,
                rather then the id, as the suffix for the final URL
            url_suffix_from_harvested:
                Optional; If True, get the URL suffix
                from metadata.harvested rather than metadata.structured
            url_id_suffix_as_backup:
                Optional; If True, and the url_suffix_key is not found, the
                'meta.localId' is used as backup
            strip_base_url_slash:
                Optional; By default a slash is added to a base_url if it's not
                there, and it doesn't end with '='. Setting this to True means
                any trailing slash in the base_url is stripped.
            base_url_suffix_mapping_key:
                Optional; Use together with below parameter. This indicates
                what key to use the value from, to loop up the suffix in the
                below mapping
            base_url_suffix_mapping:
                Optional; In some cases URLs have a middle part that depends on
                the value of a specific key. The base_url suffix mapping is
                used to map the values of a specific key to a base_url
                suffix, e.g.:
                {
                    'Dataset': 'datasets',
                    'Text': 'publications'
                }
        """
        if base_url is not None and not base_url.endswith('='):
            base_url = base_url.rstrip('/')
            if not strip_base_url_slash:
                base_url += '/'  # With above, this guarantees 1 trailing slash
        self.base_url = base_url
        self.url_suffix_key = url_suffix_key
        self.url_id_suffix_as_backup = url_id_suffix_as_backup
        self.url_suffix_from_harvested = url_suffix_from_harvested
        self.base_url_suffix_mapping_key = base_url_suffix_mapping_key
        self.base_url_suffix_mapping = base_url_suffix_mapping
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.base_url is not None:
            if self.url_suffix_from_harvested:
                basedata = metadata.harvested
            else:
                basedata = metadata.structured

            if self.url_suffix_key is not None:
                suffix = basedata.get(self.url_suffix_key)
                if suffix is None:
                    if self.url_id_suffix_as_backup:
                        suffix = metadata.meta['localId']
                    else:
                        metadata.is_filtered = True
                        return
            else:
                suffix = metadata.meta['localId']

            if self.base_url_suffix_mapping_key is not None:
                value = metadata.structured[self.base_url_suffix_mapping_key]
                base_url_suffix = self.base_url_suffix_mapping[value]
                ext_base_url = self.base_url + base_url_suffix + '/'
            else:
                ext_base_url = self.base_url

            metadata.meta['url'] = ext_base_url + suffix
        super()._process(metadata)


class FormatStringUrlMixin:
    """
    Mixin to construct the URL based on a format string that's filled with the
    value of a specific key
    """
    def __init__(
            self, *args, format_url: str = None, format_url_key: str = None,
            **kwargs
            ):
        """
        Initializes the Mixin instance

        Args:
            format_url:
                Optional; The URL format string with one empty position,
                e.g. (https://domain.tld/{}/view)
            format_url_key:
                Optional; The key to use when filling the format
                url string. If not provided, the id is used.
        """
        self.format_url = format_url
        self.format_url_key = format_url_key
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.format_url is not None:
            if self.format_url_key is not None:
                url_part = _common.get_data_from_loc(
                    metadata.processed, self.format_url_key
                )
                if url_part is None:
                    raise ValueError('Url Key missing from metadata.processed')
            else:
                url_part = metadata.meta['localId']

            metadata.meta['url'] = self.format_url.format(quote_plus(url_part))


class RemoveKeysMixin:
    """
    Structurer Mixin to remove specific keys from metadata.structured
    """
    def __init__(self, *args, remove_keys: list[str] = None, **kwargs):
        """
        Initializes the Mixin instance

        Args:
            remove_keys: The keys to remove from the structured data
        """
        self.remove_keys = remove_keys
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.remove_keys is not None:
            for key in self.remove_keys:
                metadata.structured.pop(key, None)
        super()._process(metadata)


class RenameKeysMixin:
    """
    Structurer Mixin to rename specific keys in metadata.structured
    """
    def __init__(self, *args, rename_keys: list[list] = None, **kwargs):
        """
        Initializes the Mixin instance

        Args:
            rename_keys:
                Contains pairs of original keys, and the key to rename to
        """
        self.rename_keys = rename_keys
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.rename_keys is not None:
            for oldkey, newkey in self.rename_keys:
                value = metadata.structured.pop(oldkey, None)
                if value is not None:
                    metadata.structured[newkey] = value
        super()._process(metadata)


class KeyValueFilterMixin:
    """
    Structurer Mixin to filter entries that contain specific values under a
    key
    """
    _allowed_types = set(['accept', 'reject'])

    def __init__(
            self, *args, key_value_filter_options: list[dict] = None,
            kv_filter_from_harvested: bool = False, **kwargs
            ):
        """
        Initialize the Mixin instance

        Args:
            key_value_filter_options:
                Provides the options for filtering key/value combinations,
                e.g.:
                [
                    {
                        'key': {'organization': 'id'},
                        'values': ['irrelevant_organization_1'],
                        'type': 'reject'
                    },
                    {
                        'key': 'type',
                        'values': ['Dataset'],
                        'type': 'accept',
                        'should_completely_match': True
                    }
                ]
                'should_completely_match' indicates if all values (in case it's
                a list) should match the requirement (In case of the above, an
                entry with both type Dataset and Document would be rejected).
                By default, 'should_completely_match' is false.
                reject/keep_values should be simple types such as string, int,
                float or bool. Please note that the above list is evaluated as
                'and'. So if there is two times a type == 'accept' filter, both
                of them should match. This behaviour can be overriden by adding
                '"standalone_accept": True'
            kv_filter_from_harvested:
                Optional; If true, the filter is based on metadata.harvested,
                rather than metadata.structured
        """
        if key_value_filter_options is not None:
            self.key_value_filter_options = key_value_filter_options
        else:
            self.key_value_filter_options = []

        self.kv_filter_from_harvested = kv_filter_from_harvested

        # Option validation and filling
        for i, filter_option in enumerate(self.key_value_filter_options):
            if not isinstance(filter_option['key'], (str, dict)):
                raise TypeError('Invalid key in key_value_filter_options')
            filter_type = filter_option['type']
            if filter_type not in self._allowed_types:
                raise TypeError('Invalid type in key_value_filter_options')
            values = filter_option['values']
            if not isinstance(values, list):
                raise TypeError('Invalid values in key_value_filter_options')
            filter_option['values'] = set(values)
            for value in values:
                if not isinstance(value, (str, int, float, bool)):
                    raise TypeError(
                        'Invalid value type in key_value_filter_options'
                    )

            should_completely_match = filter_option.setdefault(
                'should_completely_match', False
            )
            if not isinstance(should_completely_match, bool):
                raise TypeError(
                    'Invalid should_completely_match in '
                    'key_value_filter_options'
                )

            if filter_type != 'accept':
                continue

            self.last_accept_index = i

            standalone_accept = filter_option.setdefault(
                'standalone_accept', False
            )
            if not isinstance(standalone_accept, bool):
                raise TypeError(
                    'Invalid standalone_accept in '
                    'key_value_filter_options'
                )

        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.kv_filter_from_harvested:
            basedata = metadata.harvested
        else:
            basedata = metadata.structured

        # If one standalone_accept passes, the next becomes irrelevant
        already_accepted = False
        for i, filter_option in enumerate(self.key_value_filter_options):
            # Get value shorthands
            key = filter_option['key']
            reference_values = filter_option['values']
            filter_type = filter_option['type']
            should_completely_match = filter_option['should_completely_match']

            actual_values = _common.get_data_from_loc(basedata, key)
            has_value = (actual_values is not None)

            if not has_value:
                if filter_type == 'accept' and not already_accepted:
                    metadata.is_filtered = True
                    break
                else:
                    continue

            if not isinstance(actual_values, list):
                actual_values = [actual_values]
            actual_values = set(actual_values)

            if should_completely_match:
                match = actual_values.issubset(reference_values)
            else:
                match = actual_values.intersection(reference_values)

            if filter_type == 'accept' and not already_accepted:
                if not match:
                    if not filter_option['standalone_accept']\
                            or i == self.last_accept_index:
                        metadata.is_filtered = True
                        break
                elif filter_option['standalone_accept']:
                    already_accepted = True
            elif filter_type == 'reject' and match:
                metadata.is_filtered = True
                break
        else:
            super()._process(metadata)

        return


class CKANTranslateMixin:
    """
    Structurer Mixin transposes english translated data onto the 'title' and
    'notes' keys
    """
    def _process(self, metadata: ResourceMetadata):
        # If title AND/OR description are not in the root, but in translation:
        translation_data = metadata.structured.get('translation')
        no_title = ('title' not in metadata.structured.keys())
        no_notes = ('notes' not in metadata.structured.keys())
        if (no_title or no_notes) and translation_data is not None and\
                isinstance(translation_data, dict):
            # If there is translation data, check if title or description is
            # missing, if so, then get these from translation data
            # get 'en', otherwise first in there
            transl_data = translation_data.get('en')
            if transl_data is None:
                transl_data = _common.get_first_key_data_with_len(
                    translation_data, 2
                )
            if transl_data is None:
                transl_data = translation_data.get('_')  # Used by eu_comb_data

            if transl_data is not None and isinstance(transl_data, dict):
                title_payload = transl_data.get('title')
                notes_payload = transl_data.get('notes')
                if no_title and title_payload is not None:
                    metadata.structured['title'] = title_payload
                if no_notes and notes_payload is not None:
                    metadata.structured['notes'] = notes_payload

        # If there is a notes_translated, title_translated and tags_translated
        # in English, make sure the original keys are replaced
        for key in ['notes_translated', 'title_translated', 'tags_translated']:
            value = metadata.structured.get(key)
            if value is None or not isinstance(value, dict)\
                    or 'en' not in value:
                continue

            en_payload = value['en']
            if en_payload is not None and en_payload != '':
                org_key = key.split('_')[0]
                metadata.structured[org_key] = en_payload

        super()._process(metadata)


class CleanXMLMixin:
    """
    Structurer Mixin to clean xml data
    """
    def __init__(self, *args, prefer_upper_xml_key: bool = False, **kwargs):
        """
        Initialize the Mixin Instance

        Args:
            prefer_upper_xml_key:
                Optional; See 'prefer_upper' parameter of
                _common.clean_xml_metadata
        """
        self.prefer_upper_xml_key = prefer_upper_xml_key
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        metadata.structured = _common.clean_xml_metadata(
            metadata.structured,
            prefer_upper=self.prefer_upper_xml_key
        )

        super()._process(metadata)


class RaiseKeyValueListMixin:
    """
    Structurer Mixin to raise the key value combinations specified in a list,
    to the top level
    """
    def __init__(
            self, *args, raise_key_value_list_options: list[dict] = None,
            raise_kvl_from_harvested: bool = False, **kwargs):
        """
        Initializes the Mixin instance

        Args:
            raise_key_value_list_options:
                Optional; Contains the configuration of the keys to raise, and
                the names used for the keys and values in that list. See
                example below:
                [
                    {
                        'key': 'extras',
                        'keykey': 'key',
                        'valuekey': 'value'
                    }
                ]
                Note that 'key' can also be a dict, see
                _common.get_data_from_loc for specifics
            raise_kvl_from_harvested:
                Optional; If true, the key value list is
                taken from metadata.harvested rather than metadata.structured
        """
        if raise_key_value_list_options is not None:
            self.raise_key_value_list_options = raise_key_value_list_options
        else:
            self.raise_key_value_list_options = []

        self.raise_kvl_from_harvested = raise_kvl_from_harvested

        # Option validation and filling
        for raise_option in self.raise_key_value_list_options:
            if not isinstance(raise_option['key'], (str, dict)):
                raise TypeError('Invalid key in raise_key_value_list_options')
            if not isinstance(raise_option['keykey'], (str, dict)):
                raise TypeError(
                    'Invalid keykey in raise_key_value_list_options'
                )
            if not isinstance(raise_option['valuekey'], (str, dict)):
                raise TypeError(
                    'Invalid valuekey in raise_key_value_list_options'
                )

        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.raise_kvl_from_harvested:
            basedata = metadata.harvested
        else:
            basedata = metadata.structured

        for raise_option in self.raise_key_value_list_options:
            list_ = _common.get_data_from_loc(
                basedata, raise_option['key'], pop=True, default=[]
            )
            for item in list_:
                key = item[raise_option['keykey']]
                value = item[raise_option['valuekey']]
                if isinstance(value, str):
                    try:
                        value = _common.string_conversion(value)
                    except TypeError:
                        # For rare cases where there's a malformed object
                        continue
                store_key = _common.rename_if_duplicate(
                    key, metadata.structured
                )
                metadata.structured[store_key] = value

        super()._process(metadata)


class FormatMixin:
    """
    Structurer Mixin to extract format information from an entry
    """
    def __init__(
            self, *args, format_key: Union[str, dict] = None,
            get_format_from_harvested: bool = False,
            format_accept_subvalues: bool = False, **kwargs
            ):
        """
        Initialize the Mixin instance

        Args:
            format_key:
                Location of format information (see
                _common.get_data_from_loc for specifics)
            get_format_from_harvested:
                Optional; If true, the format data is taken from the
                metadata.harvested instead of metadata.structured
            format_accept_subvalues:
                Optional; accept_subvalue parameter for
                _common.get_data_from_loc function
        """
        self.format_key = format_key
        self.format_accept_subvalues = format_accept_subvalues
        self.get_format_from_harvested = get_format_from_harvested

        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.get_format_from_harvested:
            basedata = metadata.harvested
        else:
            basedata = metadata.structured

        if self. format_key is not None\
                and 'format' not in metadata.structured:
            formats = None
            rformats = _common.get_data_from_loc(
                basedata, self.format_key,
                accept_subvalue=self.format_accept_subvalues
            )
            if isinstance(rformats, str):
                formats = [rformats]
            elif isinstance(rformats, list):
                formats = [
                    f for f in rformats
                    if isinstance(f, str) and f.strip() != ''
                ]

            if formats:
                metadata.structured['format'] = list(set(formats))

        super()._process(metadata)


class RaiseToParentMixin:
    """
    Structurer Mixin to raise the data that's under a subkey, to the parent
    key
    """
    def __init__(
            self, *args, raise_to_parent_key: dict = None,
            rtp_from_harvested: bool = False, **kwargs):
        """
        Initializes the Mixin instance

        Args:
            raise_to_parent_key:
                Optional; Location of subkey (dict) that should be raised to
                the parent key (key at top-most level of dict, see
                _common.get_data_from_loc)
            rtp_from_harvested:
                Optional; If True, the subkey data is raised from
                metadata.harvested rather than metadata.structured
        """
        self.raise_to_parent_key = raise_to_parent_key
        self.rtp_from_harvested = rtp_from_harvested

        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.rtp_from_harvested:
            basedata = metadata.harvested
        else:
            basedata = metadata.structured

        if self.raise_to_parent_key is not None:
            subkey_data = _common.get_data_from_loc(
                basedata, self.raise_to_parent_key
            )
            parent_key = next(iter(self.raise_to_parent_key))
            if isinstance(subkey_data, list):
                if subkey_data:
                    subkey_data = subkey_data[0]
                else:
                    subkey_data = None

            metadata.structured[parent_key] = subkey_data

        super()._process(metadata)


class UpdateFromKeyMixin:
    """
    Structurer Mixin to update metadata.structured, with the dict-data thats
    under specific keys. This moves that dict-data up to root level
    """
    def __init__(
            self, *args, update_from_keys: list = None,
            update_from_harvested: bool = False, **kwargs
            ):
        """
        Initializes the Mixin instance

        Args:
            update_from_keys:
                Optional; List of keys from which the data should update the
                root metadata.structured data. E.g.:
                [
                    'classification',
                    {'metadata': 'resource'}
                ]
                (for definition of keys, also see _common.get_data_from_loc)
            update_from_harvested:
                Optional; If True, the data is retrieved from
                metadata.harvested rather than metadata.structured
        """
        if update_from_keys is None:
            self.update_from_keys = []
        else:
            self.update_from_keys = update_from_keys

        self.update_from_harvested = update_from_harvested

        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        if self.update_from_harvested:
            basedata = metadata.harvested
        else:
            basedata = metadata.structured

        for key in self.update_from_keys:
            dict_ = _common.get_data_from_loc(
                basedata, key, pop=True, default={}
            )
            if isinstance(dict_, dict):
                for prop_new_key, value in dict_.items():
                    new_key = _common.rename_if_duplicate(
                        prop_new_key, metadata.structured
                    )
                    metadata.structured[new_key] = value

        super()._process(metadata)


class OAIPMHMixin(CleanXMLMixin, KeyIdMixin, KeyUrlMixin):
    """
    Mixin with shared functionality for all OAI-PMH Harvesters
    """
    def __init__(
            self, *args, metadata_loc: Union[str, dict] = None,
            id_prefix: Union[list[str], str] = None, base_url: str = None,
            **kwargs
            ):
        """
        Initializes the Mixin instance

        Args:
            metadata_loc:
                Optional; Location where the base metadata data can be
                found (see _common.get_data_from_loc for format). Optional if
                the _fill_structured(() function is overridden.
            id_prefix:
                Optional; The prefix(es) of the id that should be removed
                for creating the url, if base_url is used
            base_url:
                (See BaseUrlMixin). This structurer can handle both a
                url_key as well as a base_url (latter possibly in combination
                with id_prefix)
        """
        self.metadata_loc = metadata_loc
        self.base_url = base_url

        if isinstance(id_prefix, str):
            id_prefix = [id_prefix]
        self.id_prefix = id_prefix

        super().__init__(
            *args,
            id_key='header:identifier',
            **kwargs
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        """
        Fill metadata.structured, with the data under the 'resource' key
        """
        header = metadata.harvested.get('header')
        md = metadata.harvested.get('metadata')
        if header is None or md is None:
            metadata.is_filtered = True
            return

        metadata.structured = _common.get_data_from_loc(
            metadata.harvested, self.metadata_loc
        )
        metadata.structured.update(
            {
                ('header:' + k): v for k, v
                in metadata.harvested['header'].items()
            }
        )

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        if 'url' not in metadata.meta:
            filtered_id = ''
            # After deriving the id, use it to construct the url
            if self.id_prefix is None:
                filtered_id = re.sub(
                    r'^(.*(?<!:):(?!:))', '', metadata.meta['localId']
                )
            else:
                id_ = metadata.meta['localId']
                for prefix in self.id_prefix:
                    filtered_id = id_.removeprefix(prefix)
                    if filtered_id != id_:
                        break

            # HACK: quickfix for ANDS
            if filtered_id.startswith('ands.org.au::'):
                filtered_id = filtered_id.removeprefix('ands.org.au::')

            metadata.meta['url'] = self.base_url + filtered_id


class DataverseMixin(
        KeyIdMixin, BaseUrlMixin
        ):
    """Mixin for doing the basic tasks to structure Dataverse Data"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, id_key='global_id', **kwargs)


# CONCRETE STRUCTURER CLASSES
class JunarStructurer(
        KeyIdMixin, KeyUrlMixin, Structurer
        ):
    """Junar Data Structurer"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, id_key='guid', url_key='link', **kwargs)


class DataverseStructurer(DataverseMixin, Structurer):
    """Dataverse Data Structurer"""
    pass


class DataverseSchemaOrgStructurer(DataverseMixin, FormatMixin, Structurer):
    """Structurer for the Dataverse schema.org format"""
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, format_key={'distribution': 'fileFormat'}, **kwargs
        )


class CKANStructurer(
        KeyValueFilterMixin, KeyIdMixin, BaseUrlMixin, CKANTranslateMixin,
        RaiseKeyValueListMixin, FormatMixin, RaiseToParentMixin, Structurer
        ):
    """CKAN Data Structurer"""
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            url_suffix_key='name',
            raise_key_value_list_options=[
                {
                    'key': 'extras',
                    'keykey': 'key',
                    'valuekey': 'value',
                }
            ],
            format_key={'resources': 'format'},
            raise_to_parent_key={'organization': 'title'},
            **kwargs,
        )


class SocrataStructurer(
        KeyIdMixin, KeyUrlMixin, UpdateFromKeyMixin, RaiseKeyValueListMixin,
        Structurer
        ):
    """Socrata Data Structurer"""
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            url_key='permalink',
            url_from_harvested=True,
            update_from_keys=['classification'],
            update_from_harvested=True,
            raise_key_value_list_options=[
                {
                    'key': 'domain_metadata',
                    'keykey': 'key',
                    'valuekey': 'value',
                }
            ],
            **kwargs,
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        """
        Fill metadata.structured, with the data under the 'resource' key
        """
        metadata.structured = metadata.harvested.pop('resource')


class OAIDatacitePayloadStructurer(OAIPMHMixin, Structurer):
    """
    Structurer for OAI-PMH data with 'metadata' --> 'oai_datacite' -->
    'payload' -> 'resource' structure.
    See http://schema.datacite.org/oai/oai-1.0/oai.xsd
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            metadata_loc={
                'metadata': {
                    'oai_datacite': {
                        'payload': 'resource'
                    }
                }
            },
            **kwargs
        )


class OAIDataciteResourceStructurer(
        KeyValueFilterMixin, OAIPMHMixin, Structurer
        ):
    """
    Structurer for OAI-PMH data with 'metadata' --> 'resource' structure.
    See http://schema.datacite.org/meta/kernel-3/metadata.xsd
    """
    def __init__(self, *args, keep_types: list[str] = None, **kwargs):
        """
        Initializes the OAIDataciteResourceStructurer

        This inherits the arguments from the OAIPMHMixin and base Structurer
        classes, and adds:

        Args:
            keep_types:
                Optional; Only if these types are found in the
                entry, the entry is kept. This is an abstraction for the
                KeyValueFilterMixin
        """
        key_value_filter_options = None
        if keep_types is not None:
            key_value_filter_options = [
                {
                    'key': {'resourceType': '@resourceTypeGeneral'},
                    'values': keep_types,
                    'type': 'accept',
                    'standalone_accept': True
                },
                {
                    'key': {'resourceType': '#text'},
                    'values': keep_types,
                    'type': 'accept',
                    'standalone_accept': True
                },
            ]
        super().__init__(
            *args,
            metadata_loc={'metadata': 'resource'},
            key_value_filter_options=key_value_filter_options,
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        if metadata.is_filtered:
            return

        # Both Point and BBOX data is in weird format, reformat
        if 'geoLocationBox' in metadata.structured:
            bbox_data = metadata.structured['geoLocationBox']
            if isinstance(bbox_data, str):
                splitted = bbox_data.strip().split(' ')
                if len(splitted) == 4:
                    xmin = float(splitted[1])
                    ymin = float(splitted[0])
                    xmax = float(splitted[3])
                    ymax = float(splitted[2])

                    metadata.structured['geoLocationBox'] = {
                        'type': 'Polygon',
                        'coordinates': [
                            [
                                [xmin, ymin],
                                [xmax, ymin],
                                [xmax, ymax],
                                [xmin, ymax],
                                [xmin, ymin]
                            ]
                        ]
                    }

        elif 'geoLocationPoint' in metadata.structured:
            point_data = metadata.structured['geoLocationPoint']
            if isinstance(point_data, str):
                splitted = point_data.strip().split(' ')
                if len(splitted) == 2:
                    y = float(splitted[0])
                    x = float(splitted[1])
                    metadata.structured['geoLocationPoint'] = {
                        'type': 'Point',
                        'coordinates': [x, y]
                    }


class OAIDCStructurer(KeyValueFilterMixin, OAIPMHMixin, Structurer):
    """
    Structurer for OAI-PMH data in the Dublin Core format
    (http://www.openarchives.org/OAI/2.0/oai_dc.xsd)
    """
    def __init__(
            self, *args, keep_types: list[str] = None, **kwargs
            ):
        """
        Initialize the OAIDCStructurer instance

        This inherits the arguments from the OAIPMH Mixin and base Structurer
        classes, and adds:

        Args:
            keep_types:
                Optional; Only if these types are found in the
                entry, the entry is kept. This is an abstraction for the
                KeyValueFilterMixin
        """
        key_value_filter_options = None
        if keep_types is not None:
            key_value_filter_options = [
                {
                    'key': 'type',
                    'values': keep_types,
                    'type': 'accept'
                },
            ]
        super().__init__(
            *args,
            metadata_loc={'metadata': 'dc'},
            key_value_filter_options=key_value_filter_options,
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        if metadata.is_filtered:
            return


class OAIISO19139Structurer(
        OAIPMHMixin, UpdateFromKeyMixin, FormatMixin, Structurer
        ):
    """
    Structurer for OAI-PMH data with 'metadata' --> 'MD_Metadata' structure.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            metadata_loc={'metadata': 'MD_Metadata'},
            update_from_keys=[
                'identificationInfo',
                'citation'
            ],
            format_key={'distributionInfo': {'distributionFormat': 'name'}},
            prefer_upper_xml_key=True,
            **kwargs
        )


class ArcGISOpenDataStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for data harvested from the ArcGIS Open Data API
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, id_key='id', id_from_harvested=True, url_suffix_key='slug',
            url_id_suffix_as_backup=True, **kwargs
            )

    def _fill_structured(self, metadata: ResourceMetadata):
        metadata.structured = _common.remove_nonetypes(
            metadata.harvested['attributes']
        )


class KnoemaDCATStructurer(KeyIdMixin, KeyUrlMixin, Structurer):
    """
    Structurer for metadata in the Knoema DCAT Format
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, id_key='about', url_key='homepage', **kwargs
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        cleaned = _common.remove_keys(metadata.harvested, '@datatype')
        metadata.structured = _common.clean_xml_metadata(
            cleaned, prefer_upper=True
        )


class OpenDataSoftStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for Open Data Soft metadata
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, id_key='datasetid', id_from_harvested=True, **kwargs
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        metadata.structured = metadata.harvested['metas']


class GeonodeStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for Geonode Data
    """
    def __init__(self, *args, exclude_prefixes: list[str] = None, **kwargs):
        """
        Initializes the Geonodestructurer instance

        Inherits all parameters from the base 'Structurer' class, and adds:

        Args:
            exclude_prefixes:
                Optional; Datasets with these prefixes in their URL are
                excluded
        """
        self.exclude_prefixes = exclude_prefixes
        super().__init__(
            *args, id_key='id', url_suffix_key='detail_url',
            strip_base_url_slash=True, **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        # Check if it's not in exclude_prefixes:
        if self.exclude_prefixes is not None:
            prefix = metadata.structured['detail_url'].split(
                '/'
            )[-1].split(':')[0]
            if prefix in self.exclude_prefixes:
                metadata.is_filtered = True
                return

        # Check if they're not unpublished/unapproved
        is_published = metadata.structured.pop('is_published', True)
        is_approved = metadata.structured.pop('is_approved', True)
        if not is_published or not is_approved:
            metadata.is_filtered = True
            return

        # Remove abstract if it has a placeholder
        if metadata.structured.get('abstract') == 'No abstract provided':
            del metadata.structured['abstract']

        super()._process(metadata)


class CSWStructurer(
        CleanXMLMixin, KeyValueFilterMixin, RemoveKeysMixin, Structurer
        ):
    """
    Structurer for CSW data
    """
    def __init__(
            self, *args, base_url: str,
            reverse_corner_coordinates: bool = False,
            id_to_lower: bool = False, **kwargs
            ):
        """
        Initializes the CSWStructurer instance

        Inherits all arguments from the KeyValueFilterMixin, the
        RemoveKeysMixin and base 'Structurer' classes, and adds:

        Args:
            base_url:
                The base url used to construct the links to the resources
            reverse_corner_coordinates:
                Optional; If True, the corner coordinates are reversed
            id_to_lower:
                Optional; If true, the lowercase id is used for the URL
        """
        self.base_url = base_url
        self.reverse_corner_coordinates = reverse_corner_coordinates
        self.id_to_lower = id_to_lower
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        # Create the URL:
        dataset_id = metadata.structured.pop('identifier', None)
        if dataset_id is None:
            metadata.is_filtered = True
            return
        if self.id_to_lower:
            dataset_id = dataset_id.lower()
        if isinstance(dataset_id, list):
            dataset_id = dataset_id[0]
        if isinstance(dataset_id, dict):
            dataset_id = dataset_id[REP_TEXTKEY]
        metadata.meta['localId'] = dataset_id
        metadata.meta['url'] = self.base_url + quote_plus(dataset_id)

        if self.reverse_corner_coordinates:
            # Reverse bbox corners:
            if ('BoundingBox' in metadata.structured
                    and 'LowerCorner' in metadata.structured['BoundingBox']
                    and 'UpperCorner' in metadata.structured['BoundingBox']):
                bbox = metadata.structured['BoundingBox']
                lc = bbox['LowerCorner']
                uc = bbox['UpperCorner']
                bbox['LowerCorner'] = ' '.join(reversed(lc.split(' ')))
                bbox['UpperCorner'] = ' '.join(reversed(uc.split(' ')))


class GMDStructurer(UpdateFromKeyMixin, FormatMixin, Structurer):
    """
    Structurer for data in the CSW GMD format
    """
    def __init__(
            self, *args, base_url: str,
            remove_ids_containing: list[str] = None, **kwargs
            ):
        """
        Initializes the GMDStructurer instance

        inherits the arguments from the base 'Structurer' class and adds:

        Args:
            base_url:
                The base url for references to the resources
            remove_ids_containing:
                Optional; Remove id's that contain any of these strings
        """
        self.base_url = base_url
        self.remove_ids_containing = remove_ids_containing
        super().__init__(
            *args,
            update_from_keys=['citation'],
            format_key={
                'distributionInfo': {
                    'distributionFormat': {'name': '_content'}
                }
            },
            format_accept_subvalues=True,
            **kwargs
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        cleaned = _common.remove_keys(
            metadata.harvested, r'^@(type|(code(space|list)))$'
        )
        metadata.structured = _common.clean_xml_metadata(
            cleaned, prefer_upper=True
        )

        # Raise IdentificationInfo to top level
        id_info = metadata.structured.pop('identificationInfo')
        if isinstance(id_info, list):
            # Take item with longest length
            try:
                id_info = max(
                    [idi for idi in id_info if isinstance(idi, dict)],
                    key=lambda i: len(i)
                )
            except ValueError:
                logger.warning(
                    'Invalid list of IdentificationInfo in GMD object'
                )
                return None
        metadata.structured.update(id_info)

    def _process(self, metadata: ResourceMetadata):
        # Get ID and URL
        id_ = metadata.structured.pop('fileIdentifier', None)
        if id_ is None:
            metadata.is_filtered = True
            return
        if self.remove_ids_containing is not None:
            for id_phrase in self.remove_ids_containing:
                if id_phrase.lower() in id_.lower():
                    metadata.is_filtered = True
                    return
        if isinstance(id_, list):
            id_ = id_[0]
        if isinstance(id_, dict):
            dsid = id_.get(REP_TEXTKEY)
            if dsid is None:
                dsid = id_['CharacterString']
            id_ = dsid
        metadata.meta['localId'] = id_
        metadata.meta['url'] = self.base_url + quote_plus(id_)

        # Get type information
        type_info = metadata.structured.pop('hierarchyLevel', None)
        if type_info is not None:
            if not isinstance(type_info, list):
                type_info = [type_info]
            types = []
            for type_ in type_info:
                if type_ is not None:
                    types.append(type_)
            if len(types) > 0:
                metadata.structured['type'] = types

        # Remove datestamp
        metadata.structured.pop('dateStamp', None)

        super()._process(metadata)


class DataOneStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for DataONE data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='identifier',
            **kwargs
        )


class BlackLightStructurer(
        RemoveKeysMixin, UpdateFromKeyMixin, KeyIdMixin, BaseUrlMixin,
        Structurer
        ):
    """
    Structurer for BlackLight data
    """
    key_replace_pattern = re.compile(
        '(^(dct?|layer)_)|(_(sm?|dt|ssim|dtsi|ssi|ssm|tesim|dtsim)$)'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)
        metadata.structured = {
            self.key_replace_pattern.sub('', k): v
            for k, v in metadata.structured.items()
        }


class SimpleStructurer(
        KeyIdMixin, KeyUrlMixin, BaseUrlMixin, RenameKeysMixin, Structurer
        ):
    """
    Structure simple data formats
    """
    def __init__(self, *args, remove_from_url: str = None, **kwargs):
        """
        Initializes the SimpleStructurer Instance

        Inherits all args from the KeyIdMixin, KeyUrlMixin, BaseUrlMixin,
        RenameKeysMixin and base 'Structurer' class, and adds:

        Args:
            remove_from_url:
                Optional; If given this part is replaced in the URL string
        """
        self.remove_from_url = remove_from_url
        super().__init__(*args, **kwargs)

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        if self.remove_from_url is not None:
            metadata.meta['url'] = metadata.meta['url'].replace(
                self.remove_from_url, ''
            )


class DataGovINStructurer(KeyIdMixin, Structurer):
    """
    Structurer for data from Data.Gov.In
    """
    def __init__(self, *args, base_url: str, **kwargs):
        """
        Initializes the DataGovINStructurer instance

        Inherits all arguments from the Structurer class, and adds:

        Args:
            base_url:
                The base url to construct the urls pointing to resources on
                the website
        """
        super().__init__(*args, id_key='id', **kwargs)
        self.base_url = base_url.strip('/')

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        url_id = metadata.structured.pop('url').split('/')[-1]
        metadata.meta['url'] =\
            f"{self.base_url}/{metadata.structured['type']}/{url_id}"


class ScienceBaseStructurer(KeyIdMixin, KeyUrlMixin, Structurer):
    """
    Structurer for ScienceBase data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            url_key={'link': 'url'},
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)

        # Extract boundingbox from spatial or from facets, or point from
        # spatial
        spatial = metadata.structured.get('spatial')
        facets = metadata.structured.get('facets')
        bbox_data = None
        point_data = None
        if spatial is not None:
            if 'boundingBox' in spatial:
                bbox_data = spatial['boundingBox']
            if 'represationalPoint' in spatial:
                point_data = spatial['represationalPoint']

        if bbox_data is None and facets is not None:
            for facet in facets:
                if 'boundingBox' in facet:
                    bbox_data = facet['boundingBox']
                    break

        if bbox_data is not None:
            metadata.structured.update(bbox_data)  # Add bbox keys to root dict
        elif point_data is not None:
            metadata.structured['spatial'] =\
                {'type': 'Point', 'coordinates': point_data}

        # Extract possible temporal range data from the 'dates' key, because
        # this is not processed by the date_preparser:
        start = None
        end = None
        for date in metadata.structured.get('dates', []):
            if date['type'].lower() == 'start':
                start = date.get('dateString')
            elif date['type'].lower() == 'end':
                end = date.get('dateString')

        if start is not None:
            metadata.structured['temporal_coverage_from'] = start
        if end is not None:
            metadata.structured['temporal_coverage_to'] = end


class GeoplatformStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for Geoplatform.gov data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            **kwargs
        )


class ElasticSearchStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structurer for ElasticSearch data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='_id',
            id_from_harvested=True,
            **kwargs
        )

    def _fill_structured(self, metadata: ResourceMetadata):
        metadata.structured = metadata.harvested['_source']


class InvenioStructurer(
        UpdateFromKeyMixin, KeyIdMixin, BaseUrlMixin, Structurer
        ):
    """
    Structurer for Invenio data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            update_from_keys=['metadata'],
            id_key='id',
            **kwargs
        )


class NCEIStructurer(ElasticSearchStructurer):
    """
    Structurer for ncei.noaa.gov

    This is basically the same as the ElasticSearchStructurer, and only adds
    the '/html' suffix to the resource URL
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            base_url=(
                'https://www.ncei.noaa.gov/metadata/geoportal/rest/'
                'metadata/item/'
            ),
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)
        metadata.meta['url'] += '/html'


class MagdaStructurer(
        UpdateFromKeyMixin, KeyValueFilterMixin, KeyIdMixin,
        FormatStringUrlMixin, Structurer
        ):
    """
    Structurer for Magda.io API data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            update_from_keys=[{'aspects': 'dcat-dataset-strings'}],
            id_key='id',
            **kwargs,
        )


class RIFCSStructurer(OAIPMHMixin, Structurer):
    """
    Structure RIF CS Data from OAI-PMH endpoints
    """
    def _fill_structured(self, metadata: ResourceMetadata):
        """
        Override the default 'fill structured' of the OAIPMHMixin
        """
        header = metadata.harvested.get('header')
        md = metadata.harvested.get('metadata')
        if header is None or md is None:
            metadata.is_filtered = True
            return

        # Merge the OAI header and metadata section:
        registry_object =\
            metadata.harvested['metadata']['registryObjects']['registryObject']
        for key in ['collection', 'service']:
            if key in registry_object:
                metadata.structured = registry_object[key]
                break
        else:
            logger.warning(
                "RIF_CS: Found entry without 'collection' or 'service'"
            )
            metadata.is_filtered = True
            return

        metadata.structured.update(
            {
                ('header:' + k): v for k, v
                in metadata.harvested['header'].items()
            }
        )


class GeonetworkStructurer(KeyIdMixin, BaseUrlMixin, Structurer):
    """
    Structerer for Geonetwork Data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key={'geonet:info': 'uuid'},
            **kwargs
        )


class EUDPStructurer(
        KeyValueFilterMixin, KeyIdMixin, BaseUrlMixin, FormatMixin, Structurer
        ):
    """
    Structurer for data from the European Data Portal
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            format_key={'distributions': {'format': 'id'}},
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        """
        Add 'dataset' if a type is not defined
        """
        if 'type' not in metadata.structured:
            metadata.structured['type'] = 'Dataset'

        super()._process(metadata)


class UdataStructurer(
        KeyIdMixin, BaseUrlMixin, FormatMixin, Structurer
        ):
    """
    Structurer for data harvested from a Udata API
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='id',
            url_suffix_key='slug',
            format_key={'resources': 'format'},
            **kwargs
        )


class DataJSONStructurer(
        KeyIdMixin, FormatMixin, Structurer
        ):
    """
    Structurer for Data.json data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='identifier',
            format_key={'distribution': 'format'},
            **kwargs
        )

    def _process(self, metadata: ResourceMetadata):
        type_ = metadata.structured.pop('@type')
        if type_:
            metadata.structured['type'] = type_

        super()._process(metadata)

        metadata.meta['url'] = metadata.meta['localId']


class DcatXMLStructurer(
        KeyIdMixin, KeyUrlMixin, FormatMixin, Structurer
        ):
    """
    Structurer for DCAT XML data
    """
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            id_key='identifier',
            url_key='about',
            format_key={'distribution': {'format': 'value'}},
            **kwargs
            )

    def _fill_structured(self, metadata: ResourceMetadata):
        cleaned = _common.clean_xml_metadata(
            metadata.harvested, prefer_upper=True
        )
        metadata.structured = cleaned['Dataset']

    def _process(self, metadata: ResourceMetadata):
        super()._process(metadata)
        if metadata.meta['localId'] is None or metadata.meta['url'] is None:
            metadata.is_filtered = True
