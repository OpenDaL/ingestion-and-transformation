# -*- coding: utf-8 -*-
"""
TRANSLATION MODULE

This module includes functions related to translating the metadata from
external data sources to the correct metadata format.
"""
import re
import datetime
import html
import copy
from abc import ABC, abstractmethod
from typing import Callable, Union, Any

import html2text
from dateparser.date import DateDataParser
import unidecode
from shapely import wkt, geometry
from shapely.errors import WKTReadingError
import fastjsonschema

from metadata_ingestion import _common, _loadcfg
from metadata_ingestion import settings as st
from metadata_ingestion.resource import ResourceMetadata

# Configure HTML2Text
html2text.config.IGNORE_ANCHORS = True
html2text.config.IGNORE_IMAGES = True
html2text.config.IGNORE_EMPHASIS = True
html2text.config.BODY_WIDTH = 0

# Load configuration data
config = _loadcfg.translators()

# Compile often used regexs for performance
email_address_pattern = re.compile(r'(mailto:)?[^(@|\s)]+@[^(@|\s)]+\.\w+')
year_pattern = re.compile(r'^\d{4}$')
url_pattern = re.compile(r'https?://[^\s]*$')
between_brackets_pattern = re.compile(r'\((.*?)\)')
html_pattern = re.compile(r'<\w[^(<|>)]*>')


class FieldTranslator(ABC):
    """
    Base class for field translators
    """
    dependencies = []

    @property
    def field_name(self):
        raise NotImplementedError(
            'A field_name was not defined for this translator'
        )

    def __init__(self, fields: list[str], **kwargs):
        """
        Initialize the FieldTranslator base class instance

        Args:
            fields:
                A list of source data field names this translator should use
        """
        if self.has_circular_dependencies():
            raise TypeError(
                'Dependencies of {} are circular'.format(
                    self.__class__.__name__
                )
            )
        self.fields = fields
        # Used to determine if the translator should be used:
        self.translate_from = set(fields)
        self.properties = kwargs

    @classmethod
    def has_circular_dependencies(
            cls, previous_in_chain: set[str] = None
            ) -> bool:
        """
        Checks if the current class has circular dependencies

        Args:
            previous_in_chain:
                Optional; If called from another class, pass the upchain
                dependencies, so it can be validated these are not in the child
                dependencies

        Returns:
            Whether circular dependencies are found
        """
        if previous_in_chain is None:
            previous_in_chain = {cls.__name__}
        else:
            previous_in_chain.update(cls.__name__)

        for dependency in cls.dependencies:
            if dependency.__name__ in previous_in_chain:
                return True

            if dependency.has_circular_dependencies(
                    previous_in_chain=copy.copy(previous_in_chain)
                    ):
                return True

        return False

    def _process(self, payload: Any) -> Any:
        """
        Default function to process a single entry. Each type of data is
        delegated to the the specific processing function
        """
        if isinstance(payload, str):
            return self._process_string(payload)
        elif isinstance(payload, dict):
            return self._process_dict(payload)
        elif isinstance(payload, list):
            return self._process_list(payload)

    def translate(
            self, metadata: ResourceMetadata, preparsed_data: dict = None
            ):
        """
        Default translate function. It takes the result from the first field
        that contains valid data. This default function does not handle
        preparsed_data. Results are stored in the correct field (set by
        self.field_name of an inheriting class) in metadata.translated

        Override this method to change logic, or include logic for preparsed
        data

        Args:
            metadata:
                The metadata to translate. There should be data in
                metadata.structured to use for translation
            preparsed_data:
                Any additional key/value pairs that were assigned to this
                translator in the preparsing stage
        """
        for field in self.fields:
            if field not in metadata.structured:
                continue
            # Instead of passing the current field between the processing
            # functions, use a class variable.
            self._current_field = field
            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                metadata.translated[self.field_name] = result
                return


class Preparser(ABC):
    """
    Base class for pre parsers
    """
    def __init__(self, fields: list[str], **kwargs):
        """
        Initializes the Preparser instance

        Args:
            fields:
                The fields in metadata.structured this preparser should use
        """
        self.fields = fields
        self.properties = kwargs

    @abstractmethod
    def preparse(metadata: ResourceMetadata) -> dict:
        """
        Preparses data in metadata.structured if required.

        Args:
            metadata:
                This should have the .structured attribute filled already

        Returns:
            A dict with translator functions as keys, and the preparsed data
            that should be passed to the .translate function of the translator
            as values
        """
        pass


class MetadataTranslator:
    """
    Metadata Translator

    This class combines all Preparsers and translators
    """
    def __init__(self):
        # Initilialize the pre-parsers
        self._preparsers = []
        for classname, kwargs in config['preparsers'].items():
            preparser = globals()[classname](**kwargs)
            self._preparsers.append(preparser)

        # Initialize and order translation functions
        unordered_translators = []
        for classname, kwargs in config['translators'].items():
            translator = globals()[classname](**kwargs)
            unordered_translators.append(translator)

        self._ordered_translators = OrderedTranslators(
            unordered_translators
        ).as_list()

        # This is deepcopied on each invocation of the translate function
        self._base_translate_kwargs = {
            t.__class__.__name__: {'preparsed_data': {}}
            for t in self._ordered_translators
        }

    def translate(self, metadata: ResourceMetadata):
        """
        Translate the metadata. Uses the data from metadata.structured, and
        fills metadata.translated
        """
        translate_kwargs = copy.deepcopy(self._base_translate_kwargs)
        for preparser in self._preparsers:
            preparsed_data = preparser.preparse(metadata)
            for translator_name, translator_data in preparsed_data.items():
                translate_kwargs[translator_name]['preparsed_data'].update(
                    translator_data
                )

        for translator in self._ordered_translators:
            kwargs = translate_kwargs[translator.__class__.__name__]
            translator.translate(metadata, **kwargs)


class OrderedTranslators:
    """
    Class that handles the logic of ordering field translators based on
    dependencies. Use the as_list function to export the ordered list after
    initialization
    """
    def __init__(self, translators: list[FieldTranslator]):
        """
        Initialize the OrderedTranslators instance

        Args:
            translators:
                An unorder list of field translators, that should be ordered
                by this instance
        """
        # First add all independent translators to ordered translators,
        self.ordered_translators = []
        self.dependent_translators = {}
        translator_names = {t.__class__.__name__ for t in translators}
        for translator in translators:
            if translator.dependencies:
                # Unmatched dependencies are allowed, but should not be used
                # in ordering
                filtered_dependencies = [
                    d.__name__ for d in translator.dependencies
                    if d.__name__ in translator_names
                ]
                if filtered_dependencies:
                    translator_name = translator.__class__.__name__
                    self.dependent_translators[translator_name] = {
                        'object': translator,
                        'dependencies': filtered_dependencies
                    }
                continue
            self.ordered_translators.append(translator)

        # Now add the dependent translators in the correct order. Note that
        # this requires a while loop, because when adding one dependency, also
        # their dependencies are added, so it's unknown how many dependent
        # locations are ordered each iteration
        while self.dependent_translators:
            item = next(iter(self.dependent_translators.items()))
            self._add_independent_translator(item)

    @property
    def ordered_translator_names(self):
        return [t.__class__.__name__ for t in self.ordered_translators]

    def _add_independent_translator(
            self, dependent_translators_item: FieldTranslator
            ):
        """
        Add one of the dependent translators to the ordered translators. Takes
        an item (tuple) from the dependent translators, and adds it to the
        ordered_translators if all of it's dependencies are already in there.
        If not, it traverses down the dependency chain to find a dependency
        that can be added, untill the whole chain has been added
        """
        translator_name = dependent_translators_item[0]
        translator = dependent_translators_item[1]['object']
        dependency_names = dependent_translators_item[1]['dependencies']

        # Make sure all dependencies are in the ordered_translator_names. if
        # not, add them
        for dependency_name in dependency_names:
            if dependency_name not in self.ordered_translator_names:
                dependency_item = (
                    dependency_name,
                    self.dependent_translators[dependency_name]
                )
                self._add_independent_translator(dependency_item)

        # Add this translator
        self.ordered_translators.append(translator)
        self.dependent_translators.pop(translator_name)

    def as_list(self) -> list[FieldTranslator]:
        """
        Export the ordered translators as a list
        """
        return self.ordered_translators


def _convert_if_html(str_) -> str:
    """
    Check if a string contains html, and convert to plain text if this is the
    case
    """
    if html_pattern.search(str_):
        new_str = html2text.html2text(str_)
        new_str = html.unescape(new_str)
        return new_str
    else:
        return str_


_roles_translation = {
    'creator': ['author', 'principalinvestigator', 'coinvestigator'],
    'contact': ['pointofcontact'],
    'publisher': [
        'distributor', 'originator', 'publisher', 'resourceprovider', 'owner',
    ]
}


def _INSPIRE_role2type(role: str) -> str:
    """
    Determines the type (e.g. creator, contact or publisher) of information
    under 'role' for the INSPIRE responsible-party metadata
    """
    l_role = role.lower()

    for m_type, roles in _roles_translation.items():
        for role in roles:
            if l_role.endswith(role):
                return m_type
    else:
        return None


def _is_valid_string(
        str_, check_startswith: bool = False, check_contains: bool = False
        ) -> bool:
    """
    Validates if a string is considered 'valid'

    At it's base, it validates whether it is not in the list of 'NONE STRINGS'.

    Args:
        check_startswith:
            Optional; If True, it also checks if the string does not start
            with any of the phrases set in translators.yaml
        check_contains:
            Optional; If True, it additionally checks if the string doesn't
            contain any of the special phrases defined in translators.yaml

    Returns:
        Whether the string is considered valid
    """
    valid = True
    lstring = str_.lower()

    if lstring in config['general']['none_strings']:
        valid = False

    if check_startswith:
        for startphrase in config['general']['ignore_startswith']:
            if lstring.startswith(startphrase):
                valid = False
                break

    if check_contains:
        for text in config['general']['ignore_contains']:
            if text in lstring:
                valid = False
                break

    return valid


class DateInfoParser:
    """
    Parsing functions for date information
    """
    parse_formats = [
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%m/%d/%Y'
    ]
    parse_languages = [
        'en', 'es', 'fr', 'pt', 'de', 'nl', 'ja', 'he', 'id', 'zh', 'el', 'ru',
        'bg', 'lt', 'it', 'tr'
    ]
    now_equivalents = set(config['general']['now_equivalents'])

    # Regex patterns
    fr_date_format_pattern = re.compile(r'^\w{3},')
    iso_datetime_pattern = re.compile(
        r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-3][0-9])'
        r'([Tt]\d{2}:\d{2}:\d{2}(\.\d{3}(\d{3})?)?)?Z?$'
    )
    unseperated_date_pattern = re.compile(
        r'[0-2]\d\d\d(0[1-9]|1[0-2])(0[1-9]|1[0-9]?|2[0-9]|3[0-1])'
    )

    def __init__(
            self, greater_than: datetime.datetime,
            lower_than: datetime.datetime
            ):
        """
        Initializes the DateInfoParser

        Args:
            greater_than:
                Parse only dates greater than this date
            lower_than:
                Parse only dates lower than this date
        """
        self.gt = self._parse_date_requirement(greater_than)
        self.lt = self._parse_date_requirement(lower_than)

        self.dparser_first = DateDataParser(
            languages=self.parse_languages,
            try_previous_locales=False,
            settings={
                'PREFER_DAY_OF_MONTH': 'first',
            }
        )

        self.dparser_last = DateDataParser(
            languages=self.parse_languages,
            try_previous_locales=False,
            settings={
                'PREFER_DAY_OF_MONTH': 'last',
            }
        )

    def _parse_date_requirement(
            self, date: Union[datetime.datetime, str]
            ) -> datetime.datetime:
        """
        If the date is 'now', this will return utcnow, otherwise, it will
        return the original input
        """
        if date == 'now':
            dnow = datetime.datetime.utcnow()
            now = dnow.replace(tzinfo=datetime.timezone.utc)
            return now
        else:
            return date

    def _corrected(
            self, date: datetime.datetime
            ) -> Union[datetime.datetime, None]:
        """
        Transform the date to the UTC timezone, and check if it matches the
        requirements.
        """
        if date is not None:
            if date.tzinfo is not None:
                # If in a different timezone, convert to UTC
                date = date.astimezone(
                    datetime.timezone.utc
                )
            else:
                # If no timezone given, assume UTC. Needed for comparison below
                date = date.replace(
                    tzinfo=datetime.timezone.utc
                )

            if self.is_valid(date):
                return date
            else:
                return None

        else:
            return None

    def _parse_date_data(
            self, str_, dom_preference: str
            ) -> Union[datetime.datetime, None]:
        """
        Parse a date string using the dateparser library (Used as last resort,
        since it's slow)

        Args:
            str_:
                The data to parse
            dom_preference:
                Date of month preference, Either 'first' to
                prefer the first date when parsing a Month name, or 'last', to
                prefer the last day when parsing a month without a day
        """
        if dom_preference == 'first':
            parser = self.dparser_first
        elif dom_preference == 'last':
            parser = self.dparser_last
        else:
            raise TypeError('Invalid dom_preference argument')

        try:
            parse_result = parser.get_date_data(
                str_, date_formats=self.parse_formats
            )['date_obj']
        except OverflowError:
            return None

        return self._corrected(parse_result)

    @property
    def now(self) -> datetime.datetime:
        """
        Create Now date. This adds several days to current date, in order
        to account for Sync frequency of the portal
        """
        date = datetime.datetime.utcnow() +\
            datetime.timedelta(days=st.NOW_PDAYS)

        return date.replace(tzinfo=datetime.timezone.utc)

    def is_valid(self, date: datetime.datetime) -> bool:
        """
        Test if the given date is between the lt and gt dates
        """
        return self.gt < date < self.lt

    def parse_string(
            self, str_, period_end: bool = False, ignore_now: bool = False
            ) -> Union[datetime.datetime, None]:
        """
        Parse dates in strings to datetime objects

        Args:
            str_:
                The input to parse
            period_end:
                Optional; Set to true, if you want to parse the end of a
                period. In case e.g. you provide a year, or a month, then the
                last date/time of this year/month is used.
            ignore_now:
                Optional; If True, strings like 'now' or 'current' are not
                parsed

        Returns:
            The parsed date, when a valid value could be parsed
        """
        # First check if it's only a year:
        if len(str_) == 4 and year_pattern.match(str_):
            year = year_pattern.match(str_).group(0)
            try:
                if period_end:
                    date = datetime.datetime(int(year), 12, 31)
                else:
                    date = datetime.datetime(int(year), 1, 1)
                return self._corrected(date)
            except ValueError:
                return None
        elif self.iso_datetime_pattern.match(str_):
            # Increase performance, by catching this before passing to
            # dateparser library
            try:
                date = datetime.datetime.fromisoformat(str_.strip('Z'))
                return self._corrected(date)
            except ValueError:
                # If a too high day number for the month is used, it's bullshit
                return None
        elif (not ignore_now) and str_.lower().strip() in self.now_equivalents:
            return self.now
        elif self.fr_date_format_pattern.match(str_) is not None:
            if len(str_) > 5:
                str_ = str_[5:]
        elif self.unseperated_date_pattern.match(str_):
            try:
                date = datetime.datetime.strptime(str_, '%Y%m%d')
                return self._corrected(date)
            except ValueError:
                pass

        # Otherwise use dateparser:
        if period_end:
            return self._parse_date_data(str_, 'last')
        else:
            return self._parse_date_data(str_, 'first')

    def parse_timestamp(self, int_) -> Union[datetime.datetime, None]:
        """
        Parse a timestamp
        """
        date = None

        if len(str(int_)) > 10:
            # Likely milisecond version, convert to seconds
            int_ = int_ / 1000

        if int_ > 86400 and int_ < 9999999999:
            date = datetime.datetime.fromtimestamp(int_)

        date = self._corrected(date)

        return date

    def convert_string(self, *args, **kwargs) -> Union[str, None]:
        """
        Convert a date string to a date string in the correct format

        Args:
            *args, **kwargs:
                See DateInfoParser.parse_string
        """
        date = self.parse_string(*args, **kwargs)

        if date is not None:
            return date.strftime(st.DATE_FORMAT)
        else:
            return None


def _get_preferred_language_value(list_) -> Any:
    """
    Gets the prefered language alternative from a list of language alternatives

    Tests if a list contains options for different languages, and returns
    preferred value if true, in case it's a list of language alternatives
    either (1) the first alternative for English or (2) the first item with a
    language or (if it's not a list of language alternatives) None, meaning it
    should be further processed by other functions.
    """
    if len(list_) == 0 or not isinstance(list_[0], dict):
        # Only works for lists of dictionaries
        return
    # Check first item to get language and value keys
    language_key = None
    for lkey in config['general']['language_keys']:
        if lkey in list_[0]:
            language_key = lkey
            break
    else:
        # Not a language alternatives list
        return

    value_key = None
    for vkey in config['general']['language_value_keys']:
        if vkey in list_[0]:
            value_key = vkey
            break
    else:
        # Not a language alternatives list
        return

    # Both keys are found, so now iterate through entire list, to find first
    # English candidate. Default is the first candidate in the list
    value = list_[0][value_key]
    for item in list_:
        lval = item.get(language_key)
        if lval == 'en' or lval == '#eng':
            new_value = item.get(value_key)
            if new_value is not None:
                value = new_value
                break

    return value


def _get_value(dict_, keys: list[str], value_type: Any = None) -> Any:
    """
    Get the first value from a dict, that's under one of the 'keys'

    Args:
        dict_:
            The input dict
        keys:
            List of dictionary keys
        value_type:
            Optional; If provided, the value should be an instance of the
            give class

    Returns:
        If found, The first value found under one of the keys
    """
    for key in keys:
        if key in dict_:
            if value_type is not None:
                if isinstance(dict_[key], value_type):
                    return dict_[key]
            else:
                return dict_[key]


def get_child_schema(schema: dict, key: str) -> dict:
    """
    Get the schema for the given child key from the complete schema
    """
    if schema['type'] == 'array':
        # It's an array of objects
        return schema['items']['properties'][key]
    else:
        # It's an object
        return schema['properties'][key]


class SchemaValidationMixin:
    """
    Mixin adds the .validate function to a class, based on the 'schema' kwargs
    """
    def __init__(self, *args, schema: dict = None, **kwargs):
        """
        Initializes the SchemaValidationMixin instance

        Args:
            schema: JSON Schema definition
        """
        if schema is not None:
            # If this is used in combination with the StringTruncationMixin,
            # the schema variable may already have been set by that one
            self._schema = schema
        super().__init__(*args, **kwargs)
        self._validate = fastjsonschema.compile(self._schema)

        # Create a cache for subkey validation functions
        self._subkey_validation_functions = {}

    def _subkey_validator(self, subkey: str) -> Callable:
        """Get the validation function for the given subkey"""
        if subkey not in self._subkey_validation_functions:
            self._subkey_validation_functions[subkey] = fastjsonschema.compile(
                get_child_schema(self._schema, subkey)
            )
        return self._subkey_validation_functions[subkey]

    def is_valid(self, data, subkey: str = None) -> bool:
        """
        Checks if the provided data matches the JSON Schema, optionally for
        a given subkey
        """
        if subkey is not None:
            validate = self._subkey_validator(subkey)
        else:
            validate = self._validate
        try:
            validate(data)
            return True
        except fastjsonschema.JsonSchemaException:
            return False


class StringTruncationMixin:
    """
    Mixin that adds the .truncate_string function, based on the schema
    """
    def __init__(self, *args, schema: dict = None, **kwargs):
        """
        Initializes the StringTruncationMixin instance

        Args:
            schema:
                JSON Schema definition
        """
        if schema is not None:
            # If this is used in combination with the SchemaValidationMixin,
            # the schema variable may already have been set by that one
            self._schema = schema
        super().__init__(*args, **kwargs)
        self.min_str_length, self.max_str_length = self._get_min_max_length(
            self._schema
        )

    def _get_min_max_length(self, schema: dict) -> tuple[int, int]:
        """Return the min/max length values from the given schema"""
        min_length = schema.get('minLength', 0)
        max_length = schema.get('maxLength', 9999999)
        return min_length, max_length

    def _truncate_string(self, str_, min_length: int, max_length: int) -> str:
        """
        Truncate a string (three dots), when exceeding max_length, returns
        None is smaller than min_length
        """
        org_len = len(str_)
        new_str = None
        if org_len > max_length:
            new_str = str_[:max_length-1] + '…'
        elif org_len >= min_length:
            new_str = str_

        return new_str

    def truncate_string(self, str_, subkey: str = None) -> str:
        """
        Truncate or filter a string. If the length of the string is less than
        the minimum length, None is returned, if it's above the maximum length,
        a truncated version is returned

        If a sub-key is given, that sub-key will be generated, rather than
        using the 'minLength' and 'maxLength' properties at the root of the
        schema
        """
        if subkey is not None:
            schema = get_child_schema(self._schema, subkey)
            min_length, max_length = self._get_min_max_length(schema)
        else:
            min_length = self.min_str_length
            max_length = self.max_str_length

        org_len = len(str_)
        if org_len > max_length:
            return str_[:max_length-1] + '…'
        elif org_len >= min_length:
            return str_


class DatePreparser(Preparser):
    """
    Preparses dictionaries that can contain data for one of the date fields
    """
    def __init__(
            self, fields: list[str], *, type_translator_mapping: dict,
            datetype_keys: list[str], datevalue_keys: list[str],
            datetype_dict_keys: list[str], lt: Union[str, datetime.datetime],
            gt: Union[str, datetime.datetime]
            ):
        """
        Initializes the DatePreparser

        Args:
            fields:
                (See PreParser class)
            type_translator_mapping:
                Maping of date types to a specific
                translator
            datetype_keys:
                The keys to find date type information
            datevalue_keys:
                The keys to find the actual date under
            datetype_dict_keys:
                In case the value found under a datetype key is
                dict, these dict keys are tried to extract date type
                information
            lt:
                Detected dates should be lower than this (Either the string
                'now' or a datetime object)
            gt:
                Detected dates should be greater than this (Either the string
                'now' or a datetime object)
        """
        super().__init__(fields)
        self.type_translator_mapping = type_translator_mapping
        self.datetype_keys = datetype_keys
        self.datevalue_keys = datevalue_keys
        self.datetype_dict_keys = datetype_dict_keys
        self.parser = DateInfoParser(gt, lt)

    def _extracted_dict_data(self, dict_, preparsing_results: dict) -> bool:
        """
        Process the data and add extracted data to pre-parsing results if
        appropriate. Returns True if data was extracted from the dict, and
        false if not
        """
        # Check if the date type can be found
        translator_name = None
        for key in self.datetype_keys:
            if key in dict_:
                data = dict_[key]
                org_typenames = []
                if isinstance(data, str):
                    org_typenames.append(data.lower())
                elif isinstance(data, dict):
                    for datetype_dict_key in self.datetype_dict_keys:
                        if datetype_dict_key not in data:
                            continue
                        else:
                            org_typenames.append(data[datetype_dict_key])
                else:
                    continue

                if org_typenames:
                    for org_typename in org_typenames:
                        if org_typename in self.type_translator_mapping:
                            translator_name = \
                                self.type_translator_mapping[org_typename]
                            break
                    else:
                        continue
                else:
                    continue

                # This break statement is only reached if a translator_name
                # was found
                break
        else:
            return False

        # Check if a date can be extracted
        for key in self.datevalue_keys:
            if key in dict_:
                data = dict_[key]
                if isinstance(data, str):
                    # switch to find date periods
                    if '/' in data and org_typename == "collected":
                        # If both parts are the same length, they're likely
                        # two dates
                        parts = data.split('/')
                        if len(parts) == 2 and len(parts[0]) == len(parts[1]):
                            preparsing_results['TimePeriodTranslator'] = {
                                'temporal': data
                            }
                            return
                    date = self.parser.parse_string(data)
                    if date is not None:
                        break
                elif isinstance(data, int):
                    date = self.parser.parse_timestamp(data)
                    if date is not None:
                        break
                elif isinstance(data, dict) and st.REP_TEXTKEY in data:
                    date = self.parser.parse_string(data[st.REP_TEXTKEY])
                    if date is not None:
                        break
        else:
            return False

        def_key = '_preparsed_date'
        if translator_name == 'OtherDatesTranslator':
            def_key = org_typename
        if translator_name in preparsing_results:
            if translator_name == 'ModifiedDateTranslator':
                if date > preparsing_results[translator_name][def_key]:
                    preparsing_results[translator_name][def_key] = date
                    return True
            else:
                if date < preparsing_results[translator_name][def_key]:
                    preparsing_results[translator_name][def_key] = date
                    return True
        else:
            preparsing_results[translator_name] = {def_key: date}
            return True

    def preparse(self, metadata: ResourceMetadata) -> dict:
        results = {}
        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]

            data_extracted = False
            if isinstance(payload, dict):
                if self._extracted_dict_data(payload, results):
                    data_extracted = True
            elif isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        if self._extracted_dict_data(item, results):
                            data_extracted = True

            if data_extracted:
                metadata.structured.pop(field)

        return results


class TitleTranslator(StringTruncationMixin, FieldTranslator):
    """
    Translator for the title of entries
    """
    field_name = 'title'

    def __init__(
            self, fields: list[str], *, schema: dict,
            dict_key_priority: list[str], type_keys: list[str],
            type_priority: list[str]
            ):
        """
        Initializes a TitleTranslator instance

        Inherits all parameters from the StringTruncationMixin and
        FieldTranslator and adds:

        Args:
            dict_key_priority:
                The priority list of dictionary keys to get data from
            type_keys:
                The dictionary keys that describe the type of the title, rather
                then the title itself
            type_priority:
                Priority list of the title types (found under the type_keys) to
                prioritize in translation
        """
        super().__init__(fields, schema=schema)
        self.dict_key_priority = dict_key_priority
        self.type_keys = type_keys
        self.type_priority = type_priority

    def _process_string(self, str_) -> str:
        title = self.truncate_string(str_)

        if title is not None:
            title = title.replace("\n", " ")

        return title

    def _process_dict(self, dict_) -> str:
        if 'PT_FreeText' in dict_:
            # For GMD format language alternatives
            language_options = dict_['PT_FreeText']
            if isinstance(language_options, list):
                langval = _get_preferred_language_value(language_options)
                if langval is not None:
                    title = self._process_string(langval)
                    if title is not None:
                        return title

        for dkey in self.dict_key_priority:
            value = dict_.pop(dkey, None)
            if isinstance(value, str):
                title = self._process_string(value)
                if title is not None:
                    return title
            if isinstance(value, dict):
                title = self._process_dict(value)
                if title is not None:
                    return title

        # If priority dict key not found, try all others
        for dkey, value in dict_.items():
            if dkey not in self.type_keys and isinstance(value, str):
                title = self._process_string(value)
                if title is not None:
                    return title

    def _process_list(self, list_) -> str:
        title = None
        # First try to see of it's a list of language alternatives
        langval = _get_preferred_language_value(list_)
        if langval is not None:
            title = self._process_string(langval)
            if title is not None:
                return title

        # Extract all candidates, and priority based on type
        extracted_titles_prios = []
        found_prio = False
        for item in list_:
            c_title_prio = 99999
            c_title = None
            if isinstance(item, str):
                c_title = self._process_string(item)
            elif isinstance(item, dict):
                c_title = self._process_dict(item)
                c_title_type = _get_value(
                    item, self.type_keys, value_type=str
                )
                if c_title_type in self.type_priority:
                    c_title_prio = self.type_priority.index(c_title_type)
                    if c_title is not None:
                        found_prio = True

            if c_title is not None:
                extracted_titles_prios.append(
                    (c_title, c_title_prio)
                )

        if found_prio:
            # Get description with highest priority
            highest_prio = min(extracted_titles_prios, key=lambda t: t[1])
            return highest_prio[0]
        elif len(extracted_titles_prios) > 0:
            # Return first
            return extracted_titles_prios[0][0]
        else:
            # Nothing found
            return None


class DescriptionTranslator(StringTruncationMixin, FieldTranslator):
    """
    Field translator for an abstract or description
    """
    field_name = 'description'

    # Regex patterns
    md_links_pattern = re.compile(r'\[([^(\[|\])]*)\]\s?\(([^(\(|\))]*)\)')
    manylines_pattern = re.compile(r'\n{3,}')

    def __init__(
            self, fields: list[str], *, schema: dict,
            dict_key_priority: list[str], type_keys: list[str],
            type_priority: list[str]
            ):
        """
        Initializes a DescriptionTranslator instance

        Inherits all parameters from the StringTruncationMixin and
        FieldTranslator and adds:

        Args:
            dict_key_priority:
                The priority list of dictionary keys to get data from
            type_keys:
                The dictionary keys that describe the type of the title, rather
                then the title itself
            type_priority:
                Priority list of the title types (found under the type_keys) to
                prioritize in translation
        """
        super().__init__(fields, schema=schema)
        self.dict_key_priority = dict_key_priority
        self.type_keys = type_keys
        self.type_priority = type_priority

    def _process_string(self, str_) -> str:
        if (not _is_valid_string(str_)) or str_.lower() == 'description' or\
                str_.lower() == 'abstract':
            return None
        desc = _convert_if_html(str_)
        desc = self.md_links_pattern.sub(r'\1', desc)
        desc = self.manylines_pattern.sub('\n\n', desc)
        desc = desc.strip()
        return self.truncate_string(desc)

    def _process_dict(self, dict_) -> str:
        desc = None
        if 'PT_FreeText' in dict_:
            # For GMD format language alternatives
            language_options = dict_['PT_FreeText']
            if isinstance(language_options, list):
                langval = _get_preferred_language_value(language_options)
                if langval is not None:
                    desc = self._process_string(langval)
                    if desc is not None:
                        return desc

        for key in self.dict_key_priority:
            value = dict_.pop(key, None)
            if value is not None and isinstance(value, str):
                desc = self._process_string(value)
                if desc is not None:
                    break
        else:
            for key, value in dict_.items():
                if key not in self.type_keys and\
                        isinstance(value, str):
                    desc = self._process_string(value)
                    if desc is not None:
                        break

        return desc

    def _process_list(self, list_) -> str:
        # First try to see of it's a list of language alternatives
        langval = _get_preferred_language_value(list_)
        if langval is not None:
            desc = self._process_string(langval)
            if desc is not None:
                return desc

        # Extract all candidates, and priority based on type
        extracted_descs_prios = []
        found_prio = False
        for item in list_:
            c_desc_prio = 99999
            c_desc = None
            if isinstance(item, str):
                c_desc = self._process_string(item)
            elif isinstance(item, dict):
                c_desc = self._process_dict(item)
                c_desc_type = _get_value(
                    item, self.type_keys, value_type=str
                )
                if c_desc_type in self.type_priority:
                    c_desc_prio = self.type_priority.index(c_desc_type)
                    if c_desc is not None:
                        found_prio = True

            if c_desc is not None:
                extracted_descs_prios.append(
                    (c_desc, c_desc_prio)
                )

        if found_prio:
            # Get description with highest priority
            highest_prio = min(extracted_descs_prios, key=lambda t: t[1])
            return highest_prio[0]
        elif len(extracted_descs_prios) > 0:
            # Return first
            return extracted_descs_prios[0][0]
        else:
            # Nothing found
            return None


class VersionTranslator(SchemaValidationMixin, FieldTranslator):
    """Field Translator for version data"""
    field_name = "version"

    def _process(self, payload) -> dict:
        """
        Currently this only supports string values, and mapping them to the
        'value' property
        """
        if isinstance(payload, str):
            if _is_valid_string(payload):
                version_data = {
                    'value': payload
                }
                if self.is_valid(version_data):
                    return version_data


class CreatorTranslator(SchemaValidationMixin, FieldTranslator):
    """Field translator for the 'creator' field"""
    field_name = "creator"

    # Regex patterns
    initials_pattern = re.compile(r'\b([A-Z]\.?){1,2}\b')
    bracketed_numbers_pattern = re.compile(r'\(\d+\)')

    def _split_creators(self, str_) -> list[str]:
        # For now, only split authors if the string contains multi & or ;
        if str_.count(';') > 1:
            return str_.split(';')
        elif str_.count('&') > 1:
            return str_.split('&')
        else:
            return [str_]

    def _process_string(self, str_) -> list[dict]:
        if (
                (not _is_valid_string(str_)) or
                email_address_pattern.match(str_) or
                '{' in str_
                ):
            return None

        if self._current_field == 'organization':
            if self.is_valid(str_, 'organization'):
                return [{'organization': str_}]
            return

        creator_strings = self._split_creators(str_)

        creators = []
        for c_str in creator_strings:
            # Treat data as a single person's name
            if not self.is_valid(c_str, 'name') or ' ' not in c_str:
                return

            # If last name comes first, reverse order
            if c_str.count(',') == 1:
                names = [s.strip() for s in c_str.split(',')]
                last_name = names[0]
                first_name = names[1]
                if last_name.count(' ') <= 1:
                    if first_name.count(' ') == 0:
                        c_str = '{} {}'.format(first_name, last_name)
                    elif first_name.count(' ') == 1 and\
                            self.initials_pattern.search(first_name):
                        c_str = '{} {}'.format(first_name, last_name)
            # If there is a number in brackets, remove it (for figshare)
            c_str = self.bracketed_numbers_pattern.sub('', c_str).strip()

            creators.append({'name': c_str})

        return creators if creators != [] else None

    def _process_dict(self, dict_) -> list[dict]:
        # Three cases: (1) name key, possibly with roles or type key (2) Name
        # (capital) key or Organisation with Role, (3) creatorName key,
        # possibly with affiliation and id data
        if 'name' in dict_:
            name = dict_['name']
            base_data = None
            if isinstance(name, str):
                roles = dict_.get('roles')
                type = dict_.get('type')
                if isinstance(roles, list):
                    std_roles = [_INSPIRE_role2type(r) for r in roles]
                    if 'creator' in std_roles:
                        self._current_field = 'organization'
                        base_data = self._process_string(name)
                elif isinstance(type, str):
                    if 'organization' in type.lower():
                        self._current_field = 'organization'
                        base_data = self._process_string(name)
                    else:
                        base_data = self._process_string(name)
                else:
                    base_data = self._process_string(name)

                if base_data is not None and 'affiliation' in dict_:
                    aff = dict_['affiliation']
                    if self.is_valid(aff, subkey='affiliation'):
                        for creator in base_data:
                            creator['affiliation'] = aff

                return base_data

        elif 'Name' in dict_ or 'Organisation' in dict_:
            if 'Name' in dict_:
                name = dict_['Name']
                self._current_field = 'creator'
            else:
                name = dict_['Organisation']
                self._current_field = 'organization'

            role = dict_.get('Role')
            if isinstance(role, str):
                std_role = _INSPIRE_role2type(role)
                if not std_role == 'creator':
                    return None

            if isinstance(name, str):
                return self._process_string(name)

        elif 'givenName' in dict_ and 'familyName' in dict_:
            name = dict_['givenName']
            fname = dict_['familyName']
            if not (isinstance(name, str) and isinstance(fname, str)):
                return None

            base_data = self._process_string(f"{name} {fname}")

            if base_data is None:
                return None

            if 'affiliation' in dict_:
                aff = dict_['affiliation']
                if self.is_valid(aff, subkey='affiliation'):
                    for creator in base_data:
                        creator['affiliation'] = aff

            return base_data

        elif 'creatorName' in dict_ or 'authorName' in dict_:
            if 'creatorName' in dict_:
                name = dict_['creatorName']
            else:
                name = dict_['authorName']
            base_data = None
            if isinstance(name, str):
                base_data = self._process_string(name)

            if base_data is None:
                return None

            aff = None
            if 'affiliation' in dict_:
                aff = dict_['affiliation']
            elif 'authorAffiliation' in dict_:
                aff = dict_['authorAffiliation']

            if self.is_valid(aff, subkey='affiliation'):
                for creator in base_data:
                    creator['affiliation'] = aff

            if 'nameIdentifier' in dict_:
                id_data = dict_['nameIdentifier']
                if isinstance(id_data, dict):
                    # Check completeness:
                    identifier_scheme = None
                    id_scheme = id_data.get('nameIdentifierScheme')
                    if self.is_valid(id_scheme, subkey='identifierScheme'):
                        identifier_scheme = id_scheme
                        idfr = id_data.get(st.REP_TEXTKEY)
                        if self.is_valid(idfr, subkey='identifier'):
                            identifier = idfr.replace('-', '')
                            base_data[0]['identifierScheme'] = \
                                identifier_scheme
                            base_data[0]['identifier'] = identifier

            return base_data

        return None

    def _process_list(self, list_) -> list[dict]:
        creators = []
        for item in list_:
            if isinstance(item, str):
                result = self._process_string(item)
            elif isinstance(item, dict):
                result = self._process_dict(item)
            else:
                continue

            if result is not None:
                creators.extend(result)
        return creators if creators != [] else None

    def _process(self, payload) -> list[dict]:
        result = super()._process(payload)
        if result is not None and len(result) <= self._schema['maxItems']:
            return result
        else:
            return None


class PublisherTranslator(SchemaValidationMixin, FieldTranslator):
    """Field Translator for the publisher field"""
    field_name = 'publisher'

    def __init__(
            self, fields: list[str], *, schema: dict,
            dict_key_priority: list[str], url_keys: list[str]
            ):
        """
        Initializes the PublisherTranslator instance

        Inherits all parameters from the SchemaValidationMixin and
        FieldTranslator and adds:

        Args:
            dict_key_priority:
                The priority list of dictionary keys to extract data from
            url_keys:
                Dictionary keys that may contain URLs related to the publisher
        """
        super().__init__(fields, schema=schema)
        self.dict_key_priority = dict_key_priority
        self.url_keys = url_keys

    def _process_string(self, str_) -> dict:
        str_ = str_.strip()
        if (
                self.is_valid(str_, subkey='name') and
                _is_valid_string(str_, check_startswith=True) and
                not email_address_pattern.match(str_) and
                not url_pattern.match(str_)
                ):
            return {'name': str_}

    def _process_dict(self, dict_) -> dict:
        pub = None
        for key in self.dict_key_priority:
            if key not in dict_:
                continue

            data = dict_[key]
            if isinstance(data, str):
                result = self._process_string(data)
                if result:
                    pub = result
                    break
            elif isinstance(data, dict) and 'en' in data:
                result = self._process_string(data['en'])
                if result:
                    pub = result
                    break

        if pub is not None:
            for key in self.url_keys:
                if key not in dict_:
                    continue

                data = dict_[key]
                if isinstance(data, str):
                    is_url = url_pattern.match(data)
                    if is_url and self.is_valid(data, subkey='identifier'):
                        pub['identifier'] = data
                        pub['identifierType'] = 'URL'
            if 'role' in dict_ and isinstance(dict_['role'], str):
                p_type = _INSPIRE_role2type(dict_['role'])
                if p_type is not None and p_type != 'publisher':
                    pub = None
            elif 'roles' in dict_ and isinstance(dict_['roles'], list):
                for role in dict_['roles']:
                    p_type = _INSPIRE_role2type(role)
                    if p_type is not None and p_type != 'publisher':
                        pub = None
                        break

        return pub

    def _process_list(self, list_) -> dict:
        # First try to see of it's a list of language alternatives
        langval = _get_preferred_language_value(list_)
        if langval is not None:
            data = self._process_string(langval)  # Pass dummy for 'key'
            if data is not None:
                return data

        # Otherwise try the  conventional processing method
        data = None
        for item in list_:
            if isinstance(item, dict):
                data = self._process_dict(item)
            elif isinstance(item, str):
                data = self._process_string(item)
            if data is not None:
                break

        return data


class DateTranslator(FieldTranslator):
    """
    Base class for date translators
    """
    def __init__(
            self, fields: list[str], *, lt: Union[str, datetime.datetime],
            gt: Union[str, datetime.datetime], favor_earliest: bool = False
            ):
        """
        Initializes the DateTranslator instance

        Inherits parameters from the FieldTranslator, and adds:

        Args:
            lt:
                Detected dates should be lower than this (Either the string
                'now' or a datetime object)
            gt:
                Detected dates should be greater than this (Either the string
                'now' or a datetime object)
            favor_earliest:
                If True, it tries to extract the earliest date from
                the data, if False (default), if tries to extract the latest
                date
        """
        super().__init__(fields)
        self.parser = DateInfoParser(gt, lt)
        self.favor_earliest = favor_earliest

    def _is_inaccurate_date(self, str_) -> bool:
        """Determine if the date in the string is inaccurate (e.g. a year)"""
        return len(str_) == 4 and year_pattern.match(str_)

    def _process_string(self, str_) -> tuple[str, bool]:
        result = self.parser.convert_string(str_)
        if result is not None:
            return result, self._is_inaccurate_date(str_)
        else:
            return None, None

    def _process_list(self, list_) -> tuple[str, bool]:
        results = []
        for item in list_:
            if isinstance(item, str):
                result = self._process_string(item)
                if result[0] is not None:
                    results.append(
                        result
                    )

        if results:
            if self.favor_earliest:
                return min(results, key=lambda k: k[0])
            else:
                return max(results, key=lambda k: k[0])
        else:
            return None, None

    def _process_dict(self, dict_) -> tuple[str, bool]:
        if st.REP_TEXTKEY in dict_:
            payload = dict_[st.REP_TEXTKEY]
            if isinstance(payload, str):
                return self._process_string(payload)
            else:
                return None, None

        return None, None

    def _process_int(self, int_) -> tuple[str, bool]:
        dt = self.parser.parse_timestamp(int_)
        if dt is not None:
            return dt.strftime(st.DATE_FORMAT), False
        else:
            return None, None

    def _process_datetime(self, dt_object) -> tuple[str, bool]:
        if self.parser.is_valid(dt_object):
            return dt_object.strftime(st.DATE_FORMAT), False
        else:
            return None, None

    def _process(self, payload) -> tuple[str, bool]:
        if isinstance(payload, str):
            return self._process_string(payload)
        elif isinstance(payload, dict):
            return self._process_dict(payload)
        elif isinstance(payload, list):
            return self._process_list(payload)
        elif isinstance(payload, int):
            return self._process_int(payload)
        elif isinstance(payload, datetime.datetime):
            return self._process_datetime(payload)
        else:
            return None, None

    def translate(
            self, metadata: ResourceMetadata, *, preparsed_data: dict = None
            ):
        """
        The translate function is overridden to ensure the most accurate date
        is chosen
        """
        date = None
        inaccurate_date = None
        # First check the preparsed dates
        if preparsed_data:
            # Generally, there's just one preparsed data object for
            # a normal date
            for dt_object in preparsed_data.values():
                date, _ = self._process(dt_object)

        for field in self.fields:
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]

            new_date, is_inaccurate = self._process(payload)

            if new_date is None:
                continue

            if is_inaccurate:
                inaccurate_date = new_date
                continue

            # If the new date is accurate:
            if date is not None:
                if self.favor_earliest and new_date < date or\
                        (not self.favor_earliest) and new_date > date:
                    date = new_date
            else:
                date = new_date

        if date is None and inaccurate_date is not None:
            date = inaccurate_date

        if date is not None:
            metadata.translated[self.field_name] = date


class IssuedDateTranslator(DateTranslator):
    """Translator for the issued field"""
    field_name = 'issued'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, favor_earliest=True, **kwargs)


class ModifiedDateTranslator(DateTranslator):
    """Translator for the modified field"""
    field_name = 'modified'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, favor_earliest=False, **kwargs)


class CreatedDateTranslator(DateTranslator):
    """Translator for the modified field"""
    field_name = 'created'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, favor_earliest=True, **kwargs)


class OtherDatesTranslator(DateTranslator):
    """
    Translator for the 'otherDates' field
    """
    field_name = 'otherDates'

    def __init__(self, *args, type_mapping: dict, **kwargs):
        """
        Initializes a OtherDatesTranslator instance

        Inherits all arguments from the DateTranslator class and adds:

        Args:
            type_mapping:
                This maps field names (provided under the 'fields' argument) to
                date types. Allowed date types are 'Accepted', 'Copyrighted'
                and 'Submitted'
        """
        super().__init__(*args, **kwargs)
        self.type_mapping = type_mapping

    def translate(
            self, metadata: ResourceMetadata, preparsed_data: dict = None
            ):
        """
        Override DateTranslator 'translate' function, to apply the proper
        data format for otherdates
        """
        otherdates = []
        for field in self.fields:
            if preparsed_data and field in preparsed_data:
                payload = preparsed_data[field]
            elif field in metadata.structured:
                payload = metadata.structured[field]
            else:
                continue

            date_type = self.type_mapping[field]

            new_date, is_inaccurate = self._process(payload)

            if new_date is None:
                continue

            is_accurate = not is_inaccurate

            # Find if there's already a date with this type defined
            existing_dates = [
                d for d in otherdates if d['type'] == date_type
            ]
            if existing_dates:
                # There is max 1 existing date of the same type
                ex_date = existing_dates[0]['value']
                ex_date_accurate = existing_dates[0]['isAccurate']
                if ex_date_accurate and not is_accurate:
                    # If the existing is accurate and the current is not,
                    # drop the current
                    continue
                elif (
                        (is_accurate and not ex_date_accurate) or
                        (self.favor_earliest and new_date < ex_date) or
                        ((not self.favor_earliest) and new_date > ex_date)
                        ):
                    # Remove existing date
                    otherdates = [
                        d for d in otherdates if d['type'] != date_type
                    ]
                else:
                    continue

            # Add new date
            otherdates.append({
                'type': date_type,
                'value': new_date,
                'isAccurate': is_accurate
            })

        for date in otherdates:
            date.pop('isAccurate')  # remove internal variable

        if otherdates != []:
            metadata.translated[self.field_name] = otherdates


# Parent classes are reordered, so the __init__function of the FieldTranslator
# is skipped. Otherwise, triggering the SchemaValidationMixin __init__ would
# also trigger the FieldTranslator __init__
class ContactTranslator(FieldTranslator, SchemaValidationMixin):
    """
    Translator for the 'contact' field
    """
    field_name = 'contact'

    # Regex patterns
    phone_pattern = re.compile(r'^\+?(\d|\s|-){5,24}$')
    name_pattern = re.compile(r'^(?=\D+$)[\w\s\.,\-]+$')

    def __init__(
            self, fields: dict, primary_pairs: list, dict_key_priorities: dict,
            schema: dict
            ):
        """
        Initializes the ContactTranslator instance

        Args:
            fields:
                Each field, with an array of strings indicating whether the
                field contains a 'name', or 'details', or both.
            primary_pars:
                Describes pairs of fields that contain the name and
                the details. This is a list, where each item is a list of
                length two, containing both fields, starting with the one that
                holds the name
            dict_key_priorities:
                For each subkey (name, details), and for
                details, each detailsType, an array of the dict keys related to
                these elements.
            schema:
                (See SchemaValidationMixin)
        """
        # Since the 'fields' parameter for this class has a different layout,
        # the FieldTranslator __init__ function is skipped.
        if self.has_circular_dependencies():
            raise TypeError(
                'Dependencies of {} are circular'.format(
                    self.__class__.__name__
                )
            )
        self.fields = fields
        self.translate_from = set(fields.keys())
        SchemaValidationMixin.__init__(self, schema=schema)
        # Convert to sets, so overlap can be checked
        self.primary_pairs = [
            set(pp) for pp in primary_pairs
        ]
        # To retain order, also store the original
        self.primary_pairs_original = primary_pairs
        self.dict_key_priorities = dict_key_priorities

    def _process(self, payload):
        """Not used in this translator"""
        pass

    def _process_name_string(self, str_) -> str:
        """Process string name data"""
        if (
                (not _is_valid_string(str_)) or
                email_address_pattern.match(str_) or
                url_pattern.match(str_)
                ):
            return
        elif not self.is_valid(str_, 'name'):
            return
        else:
            if str_.count(',') > 1:
                str_opts = str_.split(',')
                for str_ in str_opts:
                    if self.name_pattern.match(str_):
                        break
                else:
                    str_ = None
            return str_

    def _process_name_dict(self, dict_) -> str:
        """Process dict name data"""
        name = None

        role = dict_.get('role')
        if isinstance(role, str):
            if _INSPIRE_role2type(role) != 'contact':
                return None

        roles = dict_.get('roles')
        if isinstance(roles, list):
            for role in roles:
                if _INSPIRE_role2type(role) == 'contact':
                    break
            else:
                return None

        for key in self.dict_key_priorities['name']:
            data = dict_.get(key)
            if isinstance(data, str):
                name = self._process_name_string(data)
                if name is not None:
                    break

        return name

    def _process_name(self, payload) -> str:
        """Process possible name data"""
        name = None
        if isinstance(payload, str):
            name = self._process_name_string(payload)
        elif isinstance(payload, dict):
            name = self._process_name_dict(payload)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    name = self._process_name_dict(item)
                    if name is not None:
                        break

        return name

    def _process_details_string(self, str_, dtype) -> str:
        if self.is_valid(str_, 'details'):
            if (
                (dtype == 'email' and not email_address_pattern.match(str_)) or
                (dtype == 'address' and not (',' in str_ or '\n' in str_)) or
                (dtype == 'phone' and self.phone_pattern.match(str_) is None)
            ):
                return None
            else:
                if dtype == 'email':
                    str_ = str_.replace('mailto:', '')
                return str_
        else:
            return None

    def _process_details_dict(self, dict_) -> tuple[str, str]:
        details = None
        details_type = None

        # Check for email adress:
        for key in self.dict_key_priorities['details']['email']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = self._process_details_string(data, 'email')
                if details is not None:
                    details_type = 'Email'
                    return details, details_type

        # Check for phone number:
        for key in self.dict_key_priorities['details']['phone']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = self._process_details_string(data, 'phone')
                if details is not None:
                    details_type = 'Phone'
                    return details, details_type

        # Check for street address:
        for key in self.dict_key_priorities['details']['address']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = self._process_details_string(data, 'address')
                if details is not None:
                    details_type = 'Address'
                    return details, details_type

        return details, details_type

    def _process_details(self, payload) -> tuple[str, str]:
        """Process Possible details data"""
        details = None
        details_type = None
        if isinstance(payload, str):
            details = self._process_details_string(payload, 'email')
            details_type = 'Email' if details is not None else None
        elif isinstance(payload, dict):
            details, details_type = self._process_details_dict(payload)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    details, details_type = self._process_details_dict(item)
                    if details is not None:
                        break

        return details, details_type

    def _duplicates_removed(self, contacts) -> list:
        """
        Cleans entries with the same 'details'. Keeps the first entries
        """
        cleaned_contacts = []
        prev_details = set()
        for contact in contacts:
            if contact['details'] not in prev_details:
                cleaned_contacts.append(contact)
                prev_details.add(contact['details'])

        return cleaned_contacts

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """
        Override translate function, to implement checking for primary pairs
        """
        structured_keys = set(metadata.structured.keys())

        # First check primary pairs
        contacts = []
        for i, pair in enumerate(self.primary_pairs):
            if pair.issubset(structured_keys):
                name_key, details_key = self.primary_pairs_original[i]
                self._current_field = name_key
                name = self._process_name(metadata.structured[name_key])
                self._current_field = details_key
                details, details_type = self._process_details(
                    metadata.structured[details_key]
                )

                if name is not None and details is not None:
                    contacts.append({
                        'name': name,
                        'details': details,
                        'detailsType': details_type
                    })

        # If nothing is found, check the fields seperately
        if contacts == []:
            # If no primary key combination yields data, strawl the individual
            # keys
            contact_data = {}
            name_added = False
            details_added = False
            for fieldname, fieldtypes in self.fields.items():
                if fieldname not in metadata.structured:
                    continue
                payload = metadata.structured[fieldname]
                self._current_field = fieldname
                for fieldtype in fieldtypes:
                    if fieldtype == 'name' and not name_added:
                        name = self._process_name(payload)
                        if name is not None:
                            contact_data['name'] = name
                            name_added = True
                    elif fieldtype == 'details' and not details_added:
                        details, details_type = self._process_details(
                            payload
                        )
                        if details is not None:
                            contact_data['details'] = details
                            contact_data['detailsType'] = details_type
                            details_added = True

                if name_added and details_added:
                    break

            if name_added and details_added:
                contacts.append(contact_data)
            else:
                contacts = None

        if contacts is not None:
            contacts = self._duplicates_removed(contacts)
            metadata.translated[self.field_name] = contacts


class LicenseTranslator(
        StringTruncationMixin, SchemaValidationMixin, FieldTranslator
        ):
    """
    Translator for the license field
    """
    field_name = 'license'

    def __init__(
            self, *args, dict_key_mapping: dict, name_starts: list[str],
            **kwargs
            ):
        """
        Initializes a LicenseTranslator instance

        Inherits all args from the StringTruncationMixin, SchemaValidationMixin
        and FieldTranslator, and adds:

        Args:
            dict_key_mapping:
                For each possible dict key, describes whether it
                contains a 'url' or 'text'
            name_starts:
                If the string starts with any of the phrases in this
                list, it's considered a name rather than content
        """
        super().__init__(*args, **kwargs)
        self.dict_key_mapping = dict_key_mapping
        self.name_starts = name_starts

    def _get_text_type(self, str_) -> str:
        """Decide whether a string is a name, license text or neither"""
        ttype = None
        if ' ' in str_:
            if 6 < len(str_) <= 64:
                ttype = "name"
            elif len(str_) > 64:
                ttype = "text"
        else:
            for start in self.name_starts:
                if str_.lower().startswith(start):
                    ttype = "name"
                    break

        return ttype

    def _process_string(self, str_) -> dict:
        if url_pattern.match(str_) and self.is_valid(str_, 'content'):
            return {
                'type': 'URL',
                'content': str_
            }
        elif not _is_valid_string(
                str_, check_startswith=True, check_contains=True
                ):
            return
        else:
            cleaned_str = _convert_if_html(str_)
            cleaned_str = cleaned_str.strip()
            ttype = self._get_text_type(cleaned_str)
            if ttype == 'name':
                return {'name': cleaned_str}
            elif ttype == 'text':
                text = self.truncate_string(cleaned_str, 'content')
                return {
                    'type': 'Text',
                    'content': text
                }

    def _update_data(self, existing: dict, update: dict):
        """
        Updates the 'existing' with the 'update' data, if the 'updated'
        contains more relevant data (In place)
        """
        if 'name' not in existing and 'name' in update:
            existing['name'] = update['name']

        if 'content' in update:
            if (
                    ('content' not in existing) or
                    (
                        update['type'] == 'URL' and
                        existing['type'] == 'Text'
                    )
            ):
                existing['content'] = update['content']
                existing['type'] = update['type']

    def _process_dict(self, dict_) -> dict:
        combined_data = {}
        for key, value in dict_.items():
            if key in self.dict_key_mapping and isinstance(value, str):
                maps_to = self.dict_key_mapping[key]
                if maps_to == "url":
                    if url_pattern.match(value) and\
                            self.is_valid(value, 'content'):
                        urldata = {
                            'content': value,
                            'type': 'URL'
                        }
                        self._update_data(combined_data, urldata)
                elif maps_to == "text":
                    # If it turns out to be a URL after all, the below function
                    # will stilll pick it up...
                    text_data = self._process_string(value)
                    if text_data is not None:
                        self._update_data(combined_data, text_data)

                if len(combined_data) == 3 and combined_data['type'] == 'URL':
                    break

        if combined_data:
            return combined_data

    def _process_list(self, list_) -> dict:
        data = {}
        for item in list_:
            if isinstance(item, str):
                result = self._process_string(item)
            elif isinstance(item, dict):
                result = self._process_dict(item)
            else:
                continue

            if result is not None:
                self._update_data(data, result)
                if len(data) == 3 and data['type'] == 'URL':
                    break

        if data:
            return data

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Override default method, to merge data from multiple fields"""
        data = {}
        for field in self.fields:
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                self._update_data(data, result)
                if len(data) == 3 and data['type'] == 'URL':
                    break

        if data:
            metadata.translated[self.field_name] = data


class MaintenanceTranslator(FieldTranslator):
    """
    Translator for the maintenance field
    """
    field_name = 'maintenance'

    def __init__(
            self, *args, period_dict_keys: list[str], period_mapping: dict,
            **kwargs
            ):
        """
        Initializes a MaintenanceTranslator instance

        Inherits all arguments from the FieldTranslator and adds:

        Args:
            period_dict_keys:
                Dictionary keys that contain possible period information
            period_mapping:
                A mapping from field values to a period
        """
        super().__init__(*args, **kwargs)
        self.period_dict_keys = period_dict_keys
        self.period_mapping = period_mapping

    def _process_string(self, str_):
        if str_.startswith('http'):
            str_ = str_.split('/')[-1]

        str_ = str_.lower()

        if str_ in self.period_mapping:
            return'Updated {}'.format(self.period_mapping[str_])

    def _process_dict(self, dict_):
        for key in self.period_dict_keys:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    data = self._process_string(dat)
                    if data is not None:
                        return data

    def _process_list(self, list_):
        for item in list_:
            if isinstance(item, str):
                data = self._process_string(item)
                if data is not None:
                    return data


class IdentifierTranslator(FieldTranslator):
    """
    Translator for the maintenance field
    """
    field_name = 'identifier'

    # Regex patterns
    doi_pattern = re.compile(
        r'(((https?://)?(www.)?(dx.)?doi.org/)|(doi:))?(10\.[\d\.]+/[^\s]+)$'
    )
    isbn_pattern = re.compile(r'(isbn[=:]?\s?)?([\d\-\s]{9,17}x?)$')

    def __init__(self, *args, dict_key_priority: list[str], **kwargs):
        """
        Initializes a IdentifierTranslator instance

        Inherits all arguments from the FieldTranslator and adds:

        Args:
            dict_key_priority:
                The priority of dictionary keys to get data from
        """
        super().__init__(*args, **kwargs)
        self.dict_key_priority = dict_key_priority

    def _extract_isbn(self, str_) -> dict:
        match = self.isbn_pattern.match(str_)
        if match:
            isbn = match.group(2)
            cleaned_isbn = ''.join(
                [c for c in isbn if c.isdigit() or c == 'x']
            )

            length = len(cleaned_isbn)

            if length == 10 or length == 13:
                return {'type': 'ISBN', 'value': cleaned_isbn}

    def _process_string(self, str_) -> dict:
        lstr = str_.lower()
        if lstr == '':
            return
        elif lstr.startswith('10.') or 'doi' in lstr:
            match = self.doi_pattern.match(str_)
            return {'type': 'DOI', 'value': match.group(7)} if match else None
        elif str_[0].isdigit() or 'isbn' in lstr:
            return self._extract_isbn(str_)
        else:
            return

    def _process_dict(self, dict_) -> dict:
        for key in self.dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    result = self._process(dat)
                    if result is not None:
                        return result

    def _process_list(self, list_) -> dict:
        for item in list_:
            result = self._process(item)
            if result is not None:
                return result


class TypeTranslator(FieldTranslator):
    """
    Translator for the 'type' field
    """
    field_name = 'type'

    def __init__(
            self, *args, type_mapping: dict, dict_key_priority: list[str],
            **kwargs
            ):
        """
        Initializes the TypeTranslator instance

        Inherits all arguments from the FieldTranslator class and adds:

        Args:
            type_mapping:
                Mapping between types in the source data, and types in
                the OpenDaL classification (TODO: Add reference with options)
            dict_key_priority:
                Dictionairy keys to extract type data from
        """
        super().__init__(*args, **kwargs)
        self.type_mapping = type_mapping
        self.dict_key_priority = dict_key_priority

    def _process_string(self, str_) -> str:
        rtype = None
        str_ = str_.lower()

        if str_.startswith('http'):
            if ' ' in str_:
                types = str_.split(' ')
            else:
                types = [str_]
        else:
            types = [str_]

        for desc in types:
            if desc.startswith('http') or desc.startswith('info:'):
                desc = desc.split('/')[-1]
            if len(desc) > 32:
                continue

            desc = desc.replace(' ', '')

            if ('geo' in desc and 'nongeo' not in desc) or 'map' in desc:
                rtype = 'Dataset:Geographic'
            elif 'chart' in desc or 'table' in desc:
                rtype = 'Dataset:Tabular'
            elif 'document' in desc:
                rtype = 'Document'
            elif 'report' in desc:
                rtype = 'Document:Report'
            elif 'data' in desc and desc != 'datapaper':
                rtype = 'Dataset'

            if rtype is None:
                if desc in self.type_mapping:
                    rtype = self.type_mapping[desc]

            if rtype is None and ':' in desc:
                rtype = self._process_string(desc.split(':')[-1])

            if rtype is not None:
                break

        return rtype

    def _process_dict(self, dict_) -> str:
        data = None
        for key in self.dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    data = self._process_string(dat)
                    if data is not None:
                        break

        return data

    def _process_list(self, list_) -> str:
        # Only a single type is derived
        # If there is a list, it's reviewed whether 'Dataset' is in there
        # or an entry starting with 'Dataset:', then the entry will get
        # that one, otherwise it gets the first meaningfull entry
        types = [self._process(item) for item in list_]
        filt_types = [t for t in types if t is not None]
        if filt_types != []:
            for type_ in filt_types:
                if type_.startswith('Dataset'):
                    return type_
            else:
                return filt_types[0]

    def _get_full_hierarchy(self, type_: str) -> list:
        """Get a list, including all the parents of the given type"""
        type_parts = type_.split(':')
        all_types = []
        for i in range(1, len(type_parts) + 1):
            all_types.append(':'.join(type_parts[:i]))
        return all_types

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Override to implement getting parents"""
        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                metadata.translated[self.field_name] =\
                    self._get_full_hierarchy(result)
                return


class SubjectTranslator(SchemaValidationMixin, FieldTranslator):
    """
    Translator for the Subject data
    """
    field_name = 'subject'

    def __init__(
            self, *args, source_max_size: int, dict_key_priority: list[str],
            **kwargs
            ):
        """
        Initializes the SubjectTranslator instance

        Inherits all arguments from the SchemaValidationMixin and
        FieldTranslator classes and adds:

        Args:
            source_max_size:
                The maximum size of the source date
            dict_key_priority:
                The dict keys to extract subject data from
        """
        super().__init__(*args, **kwargs)
        self.source_max_size = source_max_size
        self.dict_key_priority = dict_key_priority

        # Initialize the subject data
        self.topic_re = re.compile(r"[-'&\._\s]+")
        self.subject_scheme_data = _loadcfg.subject_scheme()
        self.subject_mapping = {}
        self.translated_subjects = set()
        for subject_id, subject_data in self.subject_scheme_data.items():
            matches_keys = [
                k for k in subject_data if k.startswith('matches_')
            ]
            all_matches = list(set(
                [
                    self.topic_re.sub("", unidecode.unidecode(d.lower()))
                    for m_key in matches_keys for d in subject_data[m_key]
                ]
            ))
            for match in all_matches:
                if match in self.translated_subjects:
                    self.subject_mapping[match].append(subject_id)
                else:
                    self.translated_subjects.add(match)
                    self.subject_mapping[match] = [subject_id]

        for subject, subject_data in self.subject_scheme_data.items():
            prs = self.find_parents_relations(subject)
            subject_data['all_parents_relations'] = prs

    def find_parents_relations(self, subject_id: str) -> list[str]:
        """
        Find all parents and relations of a specific subject
        """
        parents_and_relations = (
            self.subject_scheme_data[subject_id]['parents'] +
            self.subject_scheme_data[subject_id]['relations']
        )
        for pr in parents_and_relations:
            new_prs = self.find_parents_relations(pr)
            parents_and_relations = list(set(parents_and_relations + new_prs))

        return parents_and_relations

    def _process_string(self, str_) -> list[str]:
        """Returns a list of standardized strings"""
        new_sample = re.sub(r'["\{\}]', '', str_).lower()

        if new_sample.count(',') > 1:
            new_sample = new_sample.split(',')
        if new_sample.count(';') > 1:
            new_sample = new_sample.split(';')
        if new_sample.count('>') > 1:
            new_sample = new_sample.split('>')

        if not isinstance(new_sample, list):
            new_sample = [new_sample]

        new_sample = [
            self.topic_re.sub('', unidecode.unidecode(s)) for s in new_sample
        ]

        return new_sample

    def _process_dict(self, dict_) -> list[str]:
        """Returns a list of standardized strings"""
        standard_strings = []
        for key in self.dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    standard_strings.extend(self._process_string(dat))
                    break
                elif isinstance(dat, list):
                    standard_strings.extend(self._process_list(dat))
                    break

        return standard_strings

    def _process_list(self, list_) -> list[str]:
        """Returns a list of standardized strings"""
        if len(list_) > self.source_max_size:
            return []

        standard_strings = []
        for item in list_:
            if isinstance(item, str):
                standard_strings.extend(self._process_string(item))
            elif isinstance(item, dict):
                standard_strings.extend(self._process_dict(item))

        return standard_strings

    def _get_string_subjects(self, str_) -> list[str]:
        if str_ in self.translated_subjects:
            return self.subject_mapping[str_]
        else:
            return []

    def _relations_removed(self, subject_set: set) -> list[str]:
        """
        Remove parents and relations from a set of subjects, keeps only
        lowest level unique subjects. Also removes relations of relations
        """
        relations_parents = set()
        for subject in subject_set:
            relations_parents.update(
                self.subject_scheme_data[subject]
                ['all_parents_relations']
            )

        return [s for s in subject_set if s not in relations_parents]

    def _parents_added(self, subject_list: list[str]) -> list[str]:
        """
        Add all parents to the list of subjects
        """
        total_subjects = set()
        for subject in subject_list:
            total_subjects.add(subject)
            total_subjects.update(
                self.subject_scheme_data[subject]['all_parents_relations']
            )
        return list(total_subjects)

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """
        Override the translation function to convert the standardized
        strings and merge data from multiple keys
        """
        subjects = set()
        for field in self.fields:
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]
            standardized_strings = self._process(payload)
            for str_ in standardized_strings:
                subjects.update(
                    self._get_string_subjects(str_)
                )

        subjects = self._relations_removed(subjects)
        if subjects and self.is_valid(subjects):
            # Add all parents, to ensure proper ES search and
            # aggregations

            # Since low_level_count is needed for scoring, it's exported. Final
            # format is done in post_processing.score, where the 'low_level' is
            # used
            metadata.translated[self.field_name] = {
                'all': self._parents_added(subjects),
                'low_level': subjects,
            }


class LocationTranslator(SchemaValidationMixin, FieldTranslator):
    """
    Translator for location data
    """
    field_name = 'location'
    bbox_kv_groups = [
        (2, 3),
        (6, 7),
        (10, 11),
        (14, 15)
    ]

    # Regex Patterns
    wkt_format_pattern =\
        re.compile(r'^((POLYGON)|(POINT)|(MULTIPOLYGON)|(MULTIPOINT))\s?\(')
    bbox_data_pattern = re.compile(
        r'^(-?\d+\.?\d*)((\s-?\d+\.?\d*){3}|((,\s?)-?\d+\.?\d*){3}|'
        r'((\|\s?)-?\d+\.?\d*){3})$'
    )
    # Below uses .join, because repeated groups cannot be accessed through
    # .group using the default re module
    bbox_key_value_pattern = re.compile(
        ''.join([r'(([a-z]+)=(-?\d{1,3}([\.,]\d+)?)[;,]\s)' for i in range(3)])
        + r'(([a-z]+)=(-?\d{1,3}([\.,]\d+)?))'
    )

    def __init__(
            self, *args, bbox_field_pairs: list[list[str]],
            bbox_key_pairs: list[list[str]], **kwargs
            ):
        """
        Initializes a LocationTranslator instance

        Inherits all arguments from the SchemaValidationMixin and
        FieldTranslator classes, and adds:

        Args:
            bbox_field_pairs:
                A list of pairs of field names. A pair is a list of
                4 field names, in the order west, south, east, north, that
                together contain the data for a bounding box
            bbox_key_pairs:
                Same as above, but for keys inside dictionary data
        """
        super().__init__(*args, **kwargs)
        self.bbox_field_pairs = bbox_field_pairs
        self.bbox_field_pair_sets = [set(pair) for pair in bbox_field_pairs]
        self.bbox_key_pairs = bbox_key_pairs
        self.bbox_key_pair_sets = [set(pair) for pair in bbox_key_pairs]
        self.translate_from.update(
            [field for pair in self.bbox_field_pairs for field in pair]
        )

    def _create_geometry(
            self, xmin: float, ymin: float, xmax: float, ymax: float
            ) -> dict:
        r_xmin = round(xmin, 2)
        r_ymin = round(ymin, 2)
        r_xmax = round(xmax, 2)
        r_ymax = round(ymax, 2)
        if r_xmin == r_xmax and r_ymin == r_ymax:
            return {
                'type': 'Point',
                'coordinates': [xmin, ymin]
            }
        else:
            return {
                'type': 'envelope',
                'coordinates': [[xmin, ymax], [xmax, ymin]]
            }

    def _create_feature(
            self, name: str = None, geometry: dict = None,
            elevation: float = None
            ) -> dict:
        all_none = True
        location = {}
        if name is not None:
            all_none = False
            location['name'] = str(name)
        if geometry is not None:
            all_none = False
            location['geometry'] = dict(geometry)
        if elevation is not None:
            all_none = False
            location['elevation'] = float(elevation)

        if all_none:
            raise ValueError('At least one argument should have a value!')

        return location

    def _location_is_valid(
            self, xmin: float, ymin: float, xmax: float, ymax: float
            ) -> bool:
        """
        Returns True if it's a bbox or a point
        """
        return (
            # Values are within bounds
            (xmin >= -180 and xmax <= 180 and ymin >= -90 and ymax <= 90) and (
                # It's a bounding box
                (
                    (xmin < xmax and ymin < ymax) and
                    not (
                        xmin == -180 and
                        xmax == 180 and
                        ymin == -90 and
                        ymax == 90
                    )

                )
                or
                # It's a point
                (xmin == xmax and ymin == ymax)
            ) and
            # not emtpy data
            not (xmin == xmax == ymin == ymax == 0)
        )

    def _create_location(self, *args) -> dict:
        if self._location_is_valid(*args):
            geometry = self._create_geometry(*args)
            return self._create_feature(geometry=geometry)

    def _locations_from_shape(self, shape: geometry.shape) -> list[dict]:
        results = []

        try:
            if 'multi' in shape.type.lower():
                items = list(shape)
                for item in items:
                    result = self._create_location(*item.bounds)
                    if result is not None:
                        results.append(result)
            else:
                result = self._create_location(*shape.bounds)
                if result is not None:
                    results.append(result)
        except TypeError:
            return []

        return results

    def _process_geojson(self, dict_) -> list[dict]:
        try:
            shape = geometry.asShape(dict_)
            if not shape.is_empty:
                return self._locations_from_shape(shape)
            else:
                return []
        except ValueError:
            return []

    def _process_wkt(self, str_) -> list[dict]:
        try:
            shape = wkt.loads(str_)
            return self._locations_from_shape(shape)
        except WKTReadingError:
            return []

    def _process_string(self, str_) -> list[dict]:
        # CASE1: GeoJSON as string
        if '"type"' in str_ and '"coordinates"' in str_:
            if not str_.startswith('{') and str_.endswith('}'):
                if str_.startswith('"') and str_.endswith(']'):
                    newstr = '{' + str_ + '}'
                    geojson_data = _common.string_conversion(newstr)
                else:
                    geojson_data = None
            else:
                geojson_data = _common.string_conversion(str_)

            if isinstance(geojson_data, dict):
                return self._process_geojson(geojson_data)
        # CASE 2: SOLR Envelope format
        elif str_.startswith('ENVELOPE('):
            coordinate_string = str_.strip('ENVELOPE() ')
            try:
                coords = [float(c) for c in coordinate_string.split(',')]
                xmin, xmax, ymax, ymin = coords
            except (ValueError):
                return []

            loc = self._create_location(xmin, ymin, xmax, ymax)
            if loc is not None:
                return [loc]
        # CASE 3: It's a WKT String
        elif self.wkt_format_pattern.match(str_):
            return self._process_wkt(str_)
        # CASE 4: It's a string describing a BBOX
        elif self.bbox_data_pattern.match(str_):
            if str_.count(',') == 3:
                xmin, ymin, xmax, ymax = str_.split(',')
            elif str_.count('|') == 3:
                xmin, ymin, xmax, ymax = str_.split('|')
            else:
                xmin, ymin, xmax, ymax = str_.split(' ')

            xmin = float(xmin)
            ymin = float(ymin)
            xmax = float(xmax)
            ymax = float(ymax)

            loc = self._create_location(xmin, ymin, xmax, ymax)
            if loc is not None:
                return [loc]
        else:
            bbox_match = self.bbox_key_value_pattern.match(
                str_.lower().strip()
            )
            if bbox_match is not None:
                bbox_dict = {}
                for key_i, value_i in self.bbox_kv_groups:
                    key = bbox_match.group(key_i)
                    value = bbox_match.group(value_i)
                    bbox_dict[key] = value
                return self._process_dict(bbox_dict)

    def _process_dict(self, dict_) -> list[dict]:
        # It's in GeoJSON format:
        if 'coordinates' in dict_:
            if 'type' in dict_ and dict_['type'] != 'envelope':
                return self._process_geojson(dict_)
            else:
                if len(dict_['coordinates']) == 2:
                    coords = dict_['coordinates']
                    for coord in coords:
                        if len(coord) != 2:
                            break
                    else:
                        xs = [c[0] for c in coords]
                        ys = [c[1] for c in coords]
                        xmin = min(xs)
                        ymin = min(ys)
                        xmax = max(xs)
                        ymax = max(ys)
                        loc = self._create_location(
                            xmin, ymin, xmax, ymax
                        )
                        if loc is not None:
                            return [loc]
        # It's in CSW/Geonetwork format:
        elif 'LowerCorner' in dict_ and 'UpperCorner' in dict_:
            lc_data = dict_['LowerCorner']
            uc_data = dict_['UpperCorner']
            if isinstance(lc_data, str) and isinstance(uc_data, str):
                lc_coords = lc_data.split(' ')
                uc_coords = uc_data.split(' ')
                if len(uc_coords) == 2 and len(lc_coords) == 2:
                    try:
                        # Because each system seems to rotate x max/min and
                        # y max/min, find them dynamically. For each system
                        # The first value of a corner represents X the second
                        # value Y
                        xvals = [float(lc_coords[0]), float(uc_coords[0])]
                        yvals = [float(lc_coords[1]), float(uc_coords[1])]
                        xmin, xmax = sorted(xvals)
                        ymin, ymax = sorted(yvals)

                        loc = self._create_location(
                            xmin, ymin, xmax, ymax
                        )
                        if loc is not None:
                            return [loc]
                    except ValueError:
                        pass
        elif 'lowerleft' in dict_ and 'upperright' in dict_:
            ll = dict_['lowerleft']
            ur = dict_['upperright']
            if isinstance(ll, dict) and isinstance(ur, dict):
                ll_coords = ll.get('Point', {}).get('coordinates', None)
                ur_coords = ur.get('Point', {}).get('coordinates', None)
                if isinstance(ll_coords, str) and isinstance(ur_coords, str):
                    try:
                        xmin, ymin = ll_coords.split(',')
                        xmax, ymax = ur_coords.split(',')
                        xmin = float(xmin)
                        ymin = float(ymin)
                        xmax = float(xmax)
                        ymax = float(ymax)
                        loc = self._create_location(
                            xmin, ymin, xmax, ymax
                        )
                        if loc is not None:
                            return [loc]
                    except ValueError:
                        pass
        else:
            # Check if any of the dictBBOXPairs are in the dict:
            dict_keys = set(dict_.keys())
            for i, pair in enumerate(self.bbox_key_pair_sets):
                if pair.issubset(dict_keys):
                    xminkey, yminkey, xmaxkey, ymaxkey =\
                        self.bbox_key_pairs[i]

                    try:
                        # If they are strings, replace any comma decimal
                        # seperators with points
                        for key in pair:
                            if isinstance(dict_[key], str):
                                dict_[key] = dict_[key].replace(',', '.')
                            elif isinstance(dict_[key], (dict, list)):
                                return []

                        xmin = float(dict_[xminkey])
                        ymin = float(dict_[yminkey])
                        xmax = float(dict_[xmaxkey])
                        ymax = float(dict_[ymaxkey])
                    except ValueError:
                        break

                    loc = self._create_location(
                        xmin, ymin, xmax, ymax
                    )
                    if loc is not None:
                        return [loc]
            else:
                # No valid bbox pairs are found, try final options
                if st.REP_TEXTKEY in dict_:
                    value = dict_[st.REP_TEXTKEY]
                    if isinstance(value, str):
                        return self._process_string(value)

                # Format in ANDS:
                if dict_.get('type') == 'coverage' and 'spatial' in dict_:
                    if isinstance(dict_['spatial'], dict):
                        return self._process(dict_['spatial'])

                # Get other keys
                fetch_key = None
                for key in ['geographicElement', 'geom']:
                    if key in dict_:
                        fetch_key = key
                        break

                if fetch_key:
                    payload = dict_[fetch_key]
                    return self._process(payload)

    def _process_list(self, list_) -> list[dict]:
        results = []
        for item in list_:
            new_results = self._process(item)
            if new_results:
                results.extend(new_results)

        return results

    def _duplicates_filtered(self, locations: list[dict]) -> list[dict]:
        duplicate_inds = set()

        # Get all envelopes, used to check if points are in them
        envelopes = [
            [
                loc['geometry']['coordinates'][0][0],
                loc['geometry']['coordinates'][1][1],
                loc['geometry']['coordinates'][1][0],
                loc['geometry']['coordinates'][0][1],
            ]
            for loc in locations
            if 'geometry' in loc and loc['geometry']['type'] == 'envelope'
        ]

        # Check bboxes, points and duplicate names
        xs = set()
        ys = set()
        xmins = set()
        ymins = set()
        xmaxs = set()
        ymaxs = set()
        for ind_, loc in enumerate(locations):
            geom = loc['geometry']
            duplicate = True
            if geom['type'] == 'envelope':
                r_xmin = round(geom['coordinates'][0][0], 2)
                r_ymin = round(geom['coordinates'][1][1], 2)
                r_xmax = round(geom['coordinates'][1][0], 2)
                r_ymax = round(geom['coordinates'][0][1], 2)

                if r_xmin not in xmins:
                    duplicate = False
                elif r_ymin not in ymins:
                    duplicate = False
                elif r_xmax not in xmaxs:
                    duplicate = False
                elif r_ymax not in ymaxs:
                    duplicate = False

                if not duplicate:
                    xmins.add(r_xmin)
                    ymins.add(r_ymin)
                    xmaxs.add(r_xmax)
                    ymaxs.add(r_ymax)
                else:
                    duplicate_inds.add(ind_)
            else:
                x = geom['coordinates'][0]
                y = geom['coordinates'][1]
                r_x = round(x, 2)
                r_y = round(y, 2)

                for env in envelopes:
                    if env[0] <= x <= env[2] and env[1] <= y <= env[3]:
                        break
                else:
                    if r_x not in xs:
                        duplicate = False
                    elif r_y not in ys:
                        duplicate = False

                if not duplicate:
                    xs.add(r_x)
                    ys.add(r_y)
                else:
                    duplicate_inds.add(ind_)

            if duplicate:
                continue

        out_locs = [
            l for ind_, l in enumerate(locations) if ind_ not in duplicate_inds
        ]

        return out_locs

    def _location_from_bbox_pair_data(
            self,
            xmin_data: Union[str, int, float],
            ymin_data:  Union[str, int, float],
            xmax_data:  Union[str, int, float],
            ymax_data:  Union[str, int, float],
            ) -> dict:
        """Create a location from the data in bbox pair fields"""
        try:
            xmin = float(xmin_data)
            ymin = float(ymin_data)
            xmax = float(xmax_data)
            ymax = float(ymax_data)
            return self._create_location(
                xmin, ymin, xmax, ymax
            )
        except ValueError:
            return

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Override to check bbox pairs, and merge results"""
        locations = []

        structured_fields = set(metadata.structured.keys())
        for i, pair in enumerate(self.bbox_field_pair_sets):
            if pair.issubset(structured_fields):
                xminkey, yminkey, xmaxkey, ymaxkey = self.bbox_field_pairs[i]
                loc = self._location_from_bbox_pair_data(
                    metadata.structured[xminkey],
                    metadata.structured[yminkey],
                    metadata.structured[xmaxkey],
                    metadata.structured[ymaxkey]
                )
                if loc is not None:
                    locations.append(loc)
                    break

        if locations:
            locations = self._duplicates_filtered(locations)
            if not self.is_valid(locations):
                return
            metadata.translated[self.field_name] = locations
            return

        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]
            new_locations = self._process(payload)
            if new_locations is not None:
                locations.extend(new_locations)

        locations = self._duplicates_filtered(locations)

        if locations:
            if not self.is_valid(locations):
                return
            metadata.translated[self.field_name] = locations


class TimePeriodTranslator(FieldTranslator):
    """
    Translator for time period data
    """
    field_name = 'timePeriod'

    # regex patterns
    # Only T and Z allowed, since these are in ISO dates:
    no_written_dates_pattern = re.compile('^[^a-zA-SU-Y]+$')

    duration_pattern = re.compile(r'p(((\d+)(y|m|d|w))|(t(\d+)(h|m|s)))')

    def __init__(
            self, *args, lt: datetime.datetime, gt: datetime.datetime,
            begin_end_field_pairs: list[list[str]], dict_key_priority: dict,
            seperators: list[str], remove_strings: list[str], **kwargs
            ):
        """
        Initializes the TimePeriodTranslator

        Inherits all arguments from the FieldTranslator and adds:

        Args:
            lt:
                Max date should be lower than this
            gt:
                Min date should be bigger than this
            begin_end_field_pairs:
                List of field pairs. A field pair is a list with the names of
                the fields containing the start and the end data
            dict_key_priority:
                A dictionary with the keys 'start' and 'end',
                each one having as value a list of dict-keys that either
                resemble a start or an end of a period
            seperators:
                List of seperators that may be between the start and the
                end-date, in case the data is a string
            remove_strings:
                If any of the strings in this list is in string data,
                remove it before further processing of the data
        """
        super().__init__(*args, **kwargs)
        self.parser = DateInfoParser(gt, lt)
        self.begin_end_field_pairs = begin_end_field_pairs
        self.begin_end_field_sets = [
            set(pair) for pair in begin_end_field_pairs
        ]
        self.dict_key_priority = dict_key_priority
        self.seperators = seperators
        self.remove_strings = remove_strings

        # Both the 'fields' and field pairs should be used
        self.translate_from.update(
            [
                field for fieldpair in self.begin_end_field_pairs
                for field in fieldpair
            ]
        )

    def _create_timeperiod(
            self, start: datetime.datetime, end: datetime.datetime
            ) -> dict:
        if (
                self.parser.is_valid(start) and self.parser.is_valid(end) and
                not start > end
                ):
            return {
                'type': 'About',
                'start': start.strftime(st.DATE_FORMAT),
                'end': end.strftime(st.DATE_FORMAT),
            }

    def _parse_ISO_duration(self, str_) -> datetime.timedelta:
        """
        Parse a simple ISO duration to timedelta object (with one date or time
        value)
        """
        match = self.duration_pattern.match(str_)
        delta = None
        if match:
            dategroup = match.group(2)
            datenumber = match.group(3)
            dateunit = match.group(4)
            timegroup = match.group(5)
            timenumber = match.group(6)
            timeunit = match.group(7)
            if dategroup:
                datenumber = int(datenumber)
                if dateunit == 'y':
                    # Approximation, average year is 365.25
                    delta = datetime.timedelta(days=datenumber*365)
                elif dateunit == 'm':
                    # Approximation, timedelta does not support months
                    delta = datetime.timedelta(days=datenumber*30)
                elif dateunit == 'w':
                    delta = datetime.timedelta(weeks=datenumber)
                elif dateunit == 'd':
                    delta = datetime.timedelta(days=datenumber)
            elif timegroup:
                timenumber = int(timenumber)
                if timeunit == 'h':
                    delta = datetime.timedelta(hours=timenumber)
                elif timeunit == 'm':
                    delta = datetime.timedelta(minutes=timenumber)
                elif timeunit == 's':
                    delta = datetime.timedelta(seconds=timenumber)

        return delta

    def _process_string(self, str_) -> list[dict]:
        start_date = None
        end_date = None
        s = str_.lower()
        for rm in self.remove_strings:
            s = s.replace(rm, '')
        if len(s) > 64:
            return []
        if s.lower().startswith('r/'):
            start_payload = s.split('/')[1]
            end_payload = 'now'
            start_date = self.parser.parse_string(
                start_payload, ignore_now=True
            )
            end_date = self.parser.parse_string(end_payload, period_end=True)
        else:
            for sep in self.seperators:
                splitted = s.split(sep)
                splitted = [s.strip() for s in splitted]
                if len(splitted) == 2:
                    if (len(splitted[0]) == len(splitted[1])) or not (
                        (self.no_written_dates_pattern.match(splitted[0])
                         and self.no_written_dates_pattern.match(splitted[1]))
                            ):
                        # The lengths of the splitted parts may only differ, if
                        # There are written dates, like day names or month
                        # names in the date part, which can differ in length
                        # between start and end-date
                        start_payload = splitted[0]
                        end_payload = splitted[1]

                        start_date = self.parser.parse_string(
                            start_payload, ignore_now=True,
                        )
                        end_date = self.parser.parse_string(
                            end_payload, period_end=True
                        )

                        if start_date is not None and end_date is not None:
                            break

            else:
                # If a start date was already found, and it doesn't end with
                # a duration, set end-date to now
                years = re.findall(r'\d{4}', s)
                parts = s.split('/')
                endswith_duration = self.duration_pattern.match(parts[-1])
                if start_date is not None and not endswith_duration:
                    end_date = self.parser.now
                elif endswith_duration or (years and len(years) == 1):
                    if endswith_duration:
                        start_payload = parts[0]
                        end_payload = self._parse_ISO_duration(parts[-1])
                    else:
                        start_payload = s.strip('/-')
                        # Assume a single day/month/year coverage
                        end_payload = start_payload

                    start_date = self.parser.parse_string(
                        start_payload, ignore_now=True,
                    )

                    if start_date is None:
                        return []

                    end_date = None
                    if isinstance(end_payload, str):
                        end_date = self.parser.parse_string(
                            end_payload, period_end=True,
                        )
                    elif isinstance(end_payload, datetime.timedelta):
                        # In case a duration is parsed
                        end_date = start_date + end_payload

                    if end_date is None:
                        # Assume now, if no other can be found
                        end_date = self.parser.now

                else:
                    return []

        tperiod = self._create_timeperiod(start_date, end_date)
        if tperiod is not None:
            return [tperiod]

        return []

    def _process_dict(self, dict_) -> list[dict]:
        timeperiod_data = {}

        for edge in ['start', 'end']:
            date_kwargs = {
                'ignore_now': True if edge == 'start' else False,
                'period_end': True if edge == 'end' else False
            }
            edge_date = None
            for key in self.dict_key_priority[edge]:
                if key in dict_:
                    payload = dict_[key]
                    if isinstance(payload, str):
                        edge_date = self.parser.parse_string(
                            payload, **date_kwargs
                        )
                    elif isinstance(payload, int):
                        edge_date = self.parser.parse_timestamp(payload)
                    if edge_date is not None:
                        timeperiod_data[edge] = edge_date
                        break

            if edge_date is None:
                if edge == 'start':
                    break  # If a start date is not found, data is invalid
                else:
                    # If it's the end, assume 'now'
                    edge_date = self.parser.now
                    timeperiod_data[edge] = edge_date
        else:
            period = self._create_timeperiod(**timeperiod_data)
            if period is not None:
                return [period]

        return []

    def _process_list(self, list_) -> list[dict]:
        data = []
        for item in list_:
            data.extend(self._process(item))

        return data

    def _process_start_end(self, start_data: Any, end_data: Any) -> dict:
        """This returns a single time_period rather than an array"""
        if not (isinstance(start_data, str) and isinstance(end_data, str)):
            return

        start_date = self.parser.parse_string(start_data, ignore_now=True)
        end_date = self.parser.parse_string(end_data, period_end=True)
        if start_date is None:
            return
        elif end_date is None:
            end_date = self.parser.now

        return self._create_timeperiod(start_date, end_date)

    def _overlapping_merged(self, time_periods: list[dict]) -> list[dict]:
        # Build a dict of overlaps for each index
        overlap_per_index = {}
        for i, time_period in enumerate(time_periods):
            overlap_per_index[i] = []
            for j, other_period in enumerate(time_periods):
                if j == i:
                    continue
                if (
                    time_period['start'] <= other_period['end'] and
                    time_period['end'] >= other_period['start']
                ):
                    overlap_per_index[i].append(j)

        # Merge all by following each index, and looking at their overlaps
        merged_time_periods = []
        while overlap_per_index:
            # Build list of all indices that overlap
            merge_indices = set()
            i = next(iter(overlap_per_index.keys()))
            merge_indices.add(i)

            # Go through the chain (e.g. if 1 overlaps with 4 and 5, then
            # check with which periods 4 and 5 overlap)
            next_in_chain = [i]
            while next_in_chain:
                new_overlap_indices = []
                for j in next_in_chain:
                    new_overlap_indices.extend(
                        overlap_per_index.pop(j)
                    )
                next_in_chain = [
                    # If it's in merge indices, it was already used
                    i for i in new_overlap_indices if i not in merge_indices
                ]
                merge_indices.update(next_in_chain)

            # Merge the periods at the given indices
            merged_start = min([
                time_periods[i]['start'] for i in merge_indices
            ])
            merged_end = max([
                time_periods[i]['end'] for i in merge_indices
            ])
            merged_period = {
                'type': 'About',
                'start': merged_start,
                'end': merged_end
            }
            merged_time_periods.append(merged_period)

        return merged_time_periods

    def translate(
            self, metadata: ResourceMetadata, preparsed_data: dict = None
            ):
        time_periods = []

        available_keys = set(metadata.structured.keys())
        for i, fieldset in enumerate(self.begin_end_field_sets):
            if not fieldset.issubset(available_keys):
                continue

            start_key, end_key = self.begin_end_field_pairs[i]
            start_payload = metadata.structured[start_key]
            end_payload = metadata.structured[end_key]
            time_period = self._process_start_end(start_payload, end_payload)
            if time_period is not None:
                time_periods.append(time_period)

        for field in self.fields:
            if preparsed_data and field in preparsed_data:
                payload = preparsed_data[field]
            elif field in metadata.structured:
                payload = metadata.structured[field]
            else:
                continue

            result = self._process(payload)
            if result is not None:
                time_periods.extend(result)

        if time_periods:
            time_periods = self._overlapping_merged(time_periods)
            metadata.translated[self.field_name] = time_periods


class FormatTranslator(FieldTranslator):
    """
    Translator for file/data-format information
    """
    field_name = 'format'

    file_format_mapping = _loadcfg.file_format_mapping()

    # Regex patterns
    non_letter_pattern = re.compile(r'[^a-zA-Z\s]+')

    def _derive_plain_extensions(self, str_) -> list[str]:
        """Derive one or more file extensions from a string"""
        data = []
        # Split by commas and slashes
        parts = re.split(r',|/', str_)

        for part in parts:
            part = part.strip()
            if 1 < len(part) < 6:
                new_part = unidecode.unidecode(
                    self.non_letter_pattern.sub('', part)
                ).upper().strip()
                space_count = sum([char.isspace() for char in new_part])
                if 1 < len(new_part) < 5 and space_count == 0\
                        and _is_valid_string(new_part):
                    data.append(new_part)
        return data

    def _process_string(self, str_) -> list[str]:
        data = []

        str_ = str_.lower().replace('zipped ', '').replace(' file', '')

        if str_ in self.file_format_mapping:
            data.append(self.file_format_mapping[str_])
        else:
            if '(' in str_:
                matches = between_brackets_pattern.findall(str_)
                for match in matches:
                    data.extend(self._derive_plain_extensions(match))
            else:
                data.extend(self._derive_plain_extensions(str_))

        return data

    def _process_list(self, list_) -> list[str]:
        data = []
        for item in list_:
            if isinstance(item, str):
                data.extend(self._process_string(item))

        return data

    def _process(self, payload: Any) -> list[str]:
        """Drop default support for dicts"""
        if isinstance(payload, str):
            return self._process_string(payload)
        elif isinstance(payload, list):
            return self._process_list(payload)

        return []

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Aggregate results of multiple fields"""
        formats = []
        for field in self.fields:
            if field not in metadata.structured:
                continue
            # Instead of passing the current field between the processing
            # functions, use a class variable.
            payload = metadata.structured[field]
            formats.extend(self._process(payload))

        if formats:
            formats = list(set(formats))
            metadata.translated[self.field_name] = formats


class LanguageTranslator(FieldTranslator):
    """
    Translator for Language data
    """
    field_name = 'language'

    # Regex patterns
    and_or_pattern = re.compile(r'\band\b|\bor\b')

    def __init__(self, *args, dict_key_priority: list[str], **kwargs):
        """
        Initializes the LanguageTranslator

        Inherits all arguments from the FieldTranslator class and adds:

        Args:
            dict_key_priority:
                The dictionary keys that may contain language data
        """
        super().__init__(*args, **kwargs)
        self.dict_key_priority = dict_key_priority

        self.language_mapping = _loadcfg.language_mapping()
        self.two_letter_language_codes = list(set(
            [v for k, v in self.language_mapping.items()]
        ))

    def _process_string(self, str_) -> list[str]:
        # First seperate the string:
        str_ = str_.lower()
        if ',' in str_:
            parts = str_.split(',')
        elif str_.startswith('http'):
            parts = [str_.split('/')[-1]]
        elif '/' in str_:
            parts = str_.split('/')
        elif ':' in str_:
            parts = str_.split(':')
        elif '-' in str_ or '_' in str_:
            parts = [str_[:2]]
        elif ' and ' in str_ or ' or ' in str_:
            parts = self.and_or_pattern.split(str_)
        else:
            # Check if there are brackets
            data_between_brackets = between_brackets_pattern.findall(str_)
            if data_between_brackets != []:
                outside_brackets =\
                    [between_brackets_pattern.sub('', str_).strip()]
                parts = data_between_brackets + outside_brackets
            else:
                parts = [str_]

        langs = []
        for part in parts:
            text = part.strip()
            if len(text) == 2:
                if text in self.two_letter_language_codes:
                    langs.append(text)
            else:
                decoded = unidecode.unidecode(text)
                if text in self.language_mapping:
                    langs.append(self.language_mapping[text])
                elif decoded in self.language_mapping:
                    langs.append(self.language_mapping[decoded])

        if langs:
            return langs

    def _process_dict(self, dict_) -> list[str]:
        langs = []
        for key in self.dict_key_priority:
            if key in dict_:
                value = dict_[key]
                if isinstance(value, str):
                    result = self._process_string(value)
                elif isinstance(value, list):
                    result = self._process_dict(value)
                else:
                    continue

                if result is not None:
                    langs.extend(result)

        if langs:
            return langs

    def _process_list(self, list_) -> list[str]:
        langs = []
        for item in list_:
            if isinstance(item, str):
                new_langs = self._process_string(item)
            elif isinstance(item, dict):
                new_langs = self._process_dict(item)
            else:
                continue

            if new_langs is not None:
                langs.extend(new_langs)

        if langs:
            return langs

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Merge results"""
        languages = []
        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                languages.extend(result)

        if languages:
            languages = list(set(languages))
            metadata.translated[self.field_name] = languages


class CoordinateSystemTranslator(FieldTranslator):
    """
    Coordinate System Translator
    """
    field_name = 'coordinateSystem'

    # Regex patterns
    integer_pattern = re.compile(r'^\d+$')
    epsg_pattern = re.compile(r'epsg:{1,2}(\d+)')
    cs_name_pattern = re.compile(r'((projcs)|(geogcs))\["(.*?)"')

    def __init__(self, *args, dict_key_priority: list[str], **kwargs):
        """
        Initializes the CoordinateSystemTranslator instance

        Inherits all arguments from the base 'FieldTranslator' and adds:

        Args:
            dict_key_priority:
                The dict keys that may contain coordinate system descriptions
        """
        super().__init__(*args, **kwargs)
        self.dict_key_priority = dict_key_priority

        self.epsg_codes = set(_loadcfg.epsg_codes())
        self.name_to_epsg = _loadcfg.name_to_epsg()

    def _process_string(self, str_) -> list[int]:
        epsg_list = []
        str_ = str_.lower().strip()
        mentioned_codes = self.epsg_pattern.findall(str_)
        if self.integer_pattern.match(str_):
            # Check if the integer is a valid EPSG code
            epsg = int(str_)
            if epsg in self.epsg_codes:
                epsg_list.append(epsg)
        elif mentioned_codes != []:
            # Use codes that are referenced to as 'EPSG:...'
            for code in mentioned_codes:
                code = int(code)
                if code in self.epsg_codes:
                    epsg_list.append(code)
        elif str_.startswith('geogcs[') or str_.startswith('projcs['):
            # Parse the projection name from WKT, and convert to EPSG:
            match = self.cs_name_pattern.match(str_)
            if match:
                name = match.group(4).lower()
                if name in self.name_to_epsg:
                    epsg_list.append(self.name_to_epsg[name])
        elif str_.startswith('wgs') and '84' in str_:
            epsg_list.append(4326)
        else:
            if str_ in self.name_to_epsg:
                epsg_list.append(self.name_to_epsg[str_])

        return epsg_list

    def _process_dict(self, dict_) -> list[int]:
        epsg_list = []
        for key in self.dict_key_priority:
            if key in dict_:
                value = dict_[key]
                result = None
                if isinstance(value, str):
                    result = self._process_string(value)
                elif isinstance(value, int):
                    if value in self.epsg_codes:
                        result = value

                if result is not None:
                    epsg_list.extend(result)
                    break

        return epsg_list

    def _process(self, payload) -> list[int]:
        if isinstance(payload, str):
            return self._process_string(payload)
        elif isinstance(payload, dict):
            return self._process_dict(payload)
        else:
            return []

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """Merge the results from multiple fields"""
        epsg_codes = []
        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]
            epsg_codes.extend(self._process(payload))

        if epsg_codes:
            metadata.translated[self.field_name] = list(set(epsg_codes))
