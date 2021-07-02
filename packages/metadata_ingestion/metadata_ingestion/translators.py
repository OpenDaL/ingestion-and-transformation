# -*- coding: utf-8 -*-
"""
TRANSLATION MODULE

This module includes functions related to translating the metadata from
external data sources to the correct metadata format.
"""
import re
import datetime
import html
import hashlib
import copy
from abc import ABC, abstractmethod
from typing import Callable, Union
# # PERFORMANCE TESTING ########
# import time
# ##############################

import html2text
from dateparser.date import DateDataParser
import unidecode
from shapely import wkt, geometry
from shapely.errors import WKTReadingError
import fastjsonschema

from metadata_ingestion import _aux, _loadcfg
from metadata_ingestion import settings as st
from metadata_ingestion.resource import ResourceMetadata

# Load configuration data
trl_rules = _loadcfg.translation_rules()
trl_mapping = _loadcfg.translation()
file_format_mapping = _loadcfg.file_format_mapping()
language_mapping = _loadcfg.language_mapping()
epsg_codes = set(_loadcfg.epsg_codes())
name_to_epsg = _loadcfg.name_to_epsg()
two_letter_language_codes = list(set([v for k, v in language_mapping.items()]))
NONE_STRINGS = set(trl_rules['_general']['none_strings'])
IGNORE_STARTSWITH = trl_rules['_general']['ignore_startswith']
IGNORE_CONTAINS = trl_rules['_general']['ignore_contains']
NOW_EQUIVS = set(trl_rules['_general']['now_equivalents'])
SECOND_ROUND_FUNCTIONS = set(['untranslated'])

class_name_mapping = {
    'abstractORdescription': 'DescriptionTranslator',
    'contact': 'ContactTranslator',
    'coordinateSystem': 'CoordinateSystemTranslator',
    'created': 'CreatedDateTranslator', 
    'creator': 'CreatorTranslator',
    'date_preparser': 'DatePreparser',
    'format': 'FormatTranslator',
    'identifier': 'IdentifierTranslater', 'issued': 'IssuedDateTranslator', 'language': 'LanguageTranslator', 'license': 'LicenseTranslator', 'location': 'LocationTranslator', 'maintenance': 'MaintenanceTranslator', 'modified': 'ModifiedDateTranslator', 'otherDates': 'OtherDateTranslator', 'publishedIn': 'PublishedInTranslator', 'publisher': 'PublisherTranslator', 'relation': 'RelationTranslator', 'subject': 'SubjectTranslator', 'timePeriod': 'TimePeriodTranslator', 'title': 'TitleTranslator', 'type': 'TypeTranslator', 'untranslated': 'OriginalLanguageTranslator', 'version': 'VersionTranslator'}


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

    def __init__(self, fields, **kwargs):
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
    def has_circular_dependencies(cls, previous_in_chain: set[str] = None):
        """
        Check if the current class has circular dependencies

        Arguments:
            previous_in_chain -- If called from another class, pass the upchain
            dependencies, so it can be validated these are not in the child
            dependencies
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

    def _process(self, payload):
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
        that contains valid data.

        Override this to change logic, or include logic for preparsed data
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
    Base class for field translators
    """
    def __init__(self, fields, **kwargs):
        self.fields = fields
        self.properties = kwargs

    @abstractmethod
    def preparse(metadata: ResourceMetadata) -> dict:
        """
        Preparses data in metadata.structured if required. Returns a dict with
        translator functions as keys, and the preparsed data that should be
        passed to the .translate function of the translator as values
        """
        pass


class MetadataTranslator:
    """
    Metadata Translator

    Arguments:
        config -- dictionary that maps original field names to preparsers or
        translators
    """
    def __init__(self, config: dict):
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
            for translator_name, translator_data in preparsed_data:
                translate_kwargs[translator_name]['preparsed_data'].update(
                    translator_data
                )

        for translator in self.ordered_translators:
            kwargs = translate_kwargs[translator.__class__.__name__]
            translator.translate(metadata, **kwargs)


class OrderedTranslators:
    """
    Class that handles the logic of ordering field translators based on
    dependencies. Use the as_list function to export the ordered list after
    initiliazation
    """
    def __init__(self, translators: list[FieldTranslator]):
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

    def _add_independent_translator(self, dependent_translators_item):
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

    def as_list(self):
        """
        Export the ordered translators as a list
        """
        return self.ordered_translators


def _parse_date_requirement(date):
    """
    If the date is 'now', this will return utcnow, otherwise, it will return
    the original input
    """
    if date == 'now':
        dnow = datetime.datetime.utcnow()
        now = dnow.replace(tzinfo=datetime.timezone.utc)
        return now
    else:
        return date


min_dates = [
    trl_rules['created']['gt'],
    trl_rules['modified']['gt'],
    trl_rules['issued']['gt']
]
max_dates = [
    _parse_date_requirement(trl_rules['created']['lt']),
    _parse_date_requirement(trl_rules['modified']['lt']),
    _parse_date_requirement(trl_rules['issued']['lt'])
]
min_date = min(min_dates)
max_date = max(max_dates)

# Load subject translation data:
topic_re = re.compile(r"[-'&\._\s]+")
subject_scheme_data = _loadcfg.subject_scheme()
subject_mapping = {}
translated_subjects = set()
for subject_id, subject_data in subject_scheme_data.items():
    matches_keys = [k for k in subject_data if k.startswith('matches_')]
    all_matches = list(set([topic_re.sub("", unidecode.unidecode(d.lower()))
                            for m_key in matches_keys for d in
                            subject_data[m_key]]))
    for match in all_matches:
        if match in translated_subjects:
            subject_mapping[match].append(subject_id)
        else:
            translated_subjects.add(match)
            subject_mapping[match] = [subject_id]


# Add full list op parents and relations to subject scheme data, to filter
# duplicates when found:
def find_parents_relations(subject_id):
    parents_and_relations = subject_scheme_data[subject_id]['parents'] +\
        subject_scheme_data[subject_id]['relations']
    for pr in parents_and_relations:
        new_prs = find_parents_relations(pr)
        parents_and_relations = list(set(parents_and_relations + new_prs))

    return parents_and_relations


for subject, subject_data in subject_scheme_data.items():
    prs = find_parents_relations(subject)
    subject_data['all_parents_relations'] = prs

# Set html2text configuration
html2text.config.IGNORE_ANCHORS = True
html2text.config.IGNORE_IMAGES = True
html2text.config.IGNORE_EMPHASIS = True
html2text.config.BODY_WIDTH = 0

# # Create a dateparser instance
PARSE_DATE_FORMATS = [
    '%d-%m-%Y',
    '%d/%m/%Y',
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%m-%d-%Y',
    '%m/%d/%Y'
]
DATE_PARSER_LANGS = ['en', 'es', 'fr', 'pt', 'de', 'nl', 'ja', 'he', 'id',
                     'zh', 'el', 'ru', 'bg', 'lt', 'it', 'tr']
dparser_begin = DateDataParser(
    languages=DATE_PARSER_LANGS,
    try_previous_locales=False,
    settings={
        'PREFER_DAY_OF_MONTH': 'first',
    }
)

dparser_end = DateDataParser(
    languages=DATE_PARSER_LANGS,
    try_previous_locales=False,
    settings={
        'PREFER_DAY_OF_MONTH': 'last',
    }
)

# Compile often used regexs for performance
html_cregex = re.compile(r'<\w[^(<|>)]*>')
md_links_cregex = re.compile(r'\[([^(\[|\])]*)\]\s?\(([^(\(|\))]*)\)')
manylines_cregex = re.compile(r'\n{3,}')
email_adress_regex = re.compile(r'(mailto:)?[^(@|\s)]+@[^(@|\s)]+\.\w+')
journal_volume_regex = re.compile(r'v\w{0,5}\.?\s?(\d+)$')
journal_issue_regex = re.compile(r'[i|n]\w{0,4}\.?\s?(\d+)$')
journal_pages_regex = re.compile(r'(p\w{0,4}\.?\s?)?(\d+)\s?-\s?(\d+)$')
journal_issn_end_regex = re.compile(r'.*[^\d](\d{4}-\d{3}[\d|x])$')
title_data_regex = re.compile(r'TITLE=([^;]*)')
volume_data_regex = re.compile(r'VOLUME=(\d+)')
issue_data_regex = re.compile(r'ISSUE=(\d+)')
frompage_data_regex = re.compile(r'STARTPAGE=(\d+)')
untilpage_data_regex = re.compile(r'ENDPAGE=(\d+)')
issn_data_regex = re.compile(r'ISSN=(\d{4}-?\d{3}[\d|X])(?=[;|$])')
year_regex = re.compile(r'^\d{4}$')
fr_date_format_regex = re.compile(r'^\w{3},')
iso_datetime_pattern = re.compile(
    r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-3][0-9])([Tt]\d{2}:\d{2}:\d{2}(\.\d{3}(\d{3})?)?)?Z?$'
)
phone_regex = re.compile(r'^\+?(\d|\s|-){5,24}$')
name_regex = re.compile(r'^(?=\D+$)[\w\s\.,\-]+$')
url_regex = re.compile(r'https?://[^\s]*$')
doi_regex = re.compile(
    r'(((https?://)?(www.)?(dx.)?doi.org/)|(doi:))?(10\.[\d\.]+/[^\s]+)$'
    )
isbn_regex = re.compile(r'(isbn[=:]?\s?)?([\d\-\s]{9,17}x?)$')
wkt_format_regex =\
    re.compile(r'^((POLYGON)|(POINT)|(MULTIPOLYGON)|(MULTIPOINT))\s?\(')
bbox_data_regex = re.compile(
    r'^(-?\d+\.?\d*)((\s-?\d+\.?\d*){3}|((,\s?)-?\d+\.?\d*){3}|((\|\s?)-?\d+\.?\d*){3})$'
)
# Below uses .join, because repeated groups cannot be accessed through .group
# using the default re module
bbox_key_value_pattern = re.compile(
    ''.join([r'(([a-z]+)=(-?\d{1,3}([\.,]\d+)?)[;,]\s)' for i in range(3)])
    + r'(([a-z]+)=(-?\d{1,3}([\.,]\d+)?))'
)
bbox_kv_groups = [
    (2, 3),
    (6, 7),
    (10, 11),
    (14, 15)
]
divide_locs_regex = re.compile(',|;|:')
between_brackets_regex = re.compile(r'\((.*?)\)')
duration_regex = re.compile(r'p(((\d+)(y|m|d|w))|(t(\d+)(h|m|s)))')
andor_regex = re.compile(r'\band\b|\bor\b')
integer_regex = re.compile(r'^\d+$')
epsg_regex = re.compile(r'epsg:{1,2}(\d+)')
cs_name_regex = re.compile(r'((projcs)|(geogcs))\["(.*?)"')
orchid_isni_regex = re.compile(r'(\d{4}-?){3}\d{3}[\dX]$', re.IGNORECASE)
initials_regex = re.compile(r'\b([A-Z]\.?){1,2}\b')
non_letter_regex = re.compile(r'[^a-zA-Z\s]+')
bracketed_numbers_regex = re.compile(r'\(\d+\)')
# Only T and Z allowed, since these are in ISO dates:
no_written_dates_pattern = re.compile('^[^a-zA-SU-Y]+$')
unseperated_date_pattern = re.compile(
    r'[0-2]\d\d\d(0[1-9]|1[0-2])(0[1-9]|1[0-9]?|2[0-9]|3[0-1])'
)


def _convert_if_html(str_):
    """
    Check if a string contains html, and convert to plain text if this is the
    case
    """
    if html_cregex.search(str_):
        new_str = html2text.html2text(str_)
        new_str = html.unescape(new_str)
        return new_str
    else:
        return str_


def tkey2fname(tkey):
    """
    Translates a key in the new metadata schema to a function name

    Arguments:
        tkey --- str: The key name in the new metadata scheme

    Returns:
        str --- The function name for translation
    """
    previous_is_upper = False
    new_str_data = []
    if tkey == 'type':
        return 'type_'
    for l in tkey:
        if l.isupper():
            if previous_is_upper:
                new_str_data[-1] = new_str_data[-1].replace('_', '').upper()
                new_str_data.append(l)
            else:
                new_str_data.append('_{}'.format(l.lower()))
            previous_is_upper = True
        else:
            new_str_data.append(l)
            previous_is_upper = False

    return ''.join(new_str_data)


def _INSPIRE_role2type(role):
    """
    Determines the type of information under 'role' for the INSPIRE
    responsible-party metadata
    """
    roles_translation = {
        'creator': ['author', 'principalinvestigator', 'coinvestigator'],
        'contact': ['pointofcontact'],
        'publisher': ['distributor', 'originator', 'publisher',
                      'resourceprovider', 'owner']
    }

    l_role = role.lower()

    for m_type, roles in roles_translation.items():
        for role in roles:
            if l_role.endswith(role):
                return m_type
    else:
        return None


def _correct_date(dt, lt, gt):
    """
    Validate date that was found, based on the lower than and greater than
    values and convert it into the correct string format
    """
    if dt is not None:
        if dt.tzinfo is not None:
            # If in a different timezone, convert to UTC
            dt = dt.astimezone(datetime.timezone.utc)
        else:
            # If no timezone given, assume UTC. Needed for comparison below
            dt = dt.replace(tzinfo=datetime.timezone.utc)

        if gt < dt < lt:
            return dt
        else:
            return None

    else:
        return None


def _date2str(date):
    """
    Converts a date to a string, adding zero padding to years if required
    """
    return date.date().isoformat()


def _is_valid_string(str_, check_startswith=False, check_contains=False):
    """
    Validates a string, by validating whether it is not in the list of 'NONE
    STRINGS'. Optionally it checks if the string does not start with any of
    the phrases set in translation rules, or that it contains any of the
    'contains' phrases in the translation rules
    """
    valid = True
    lstring = str_.lower()

    if lstring in NONE_STRINGS:
        valid = False

    if check_startswith:
        for startphrase in IGNORE_STARTSWITH:
            if lstring.startswith(startphrase):
                valid = False
                break

    if check_contains:
        for text in IGNORE_CONTAINS:
            if text in lstring:
                valid = False
                break

    return valid


def _now_date():
    """
    Create Now date. This adds several days to current date, in order
    to account for Sync frequency of the portal
    """
    date = datetime.datetime.utcnow() +\
        datetime.timedelta(days=st.NOW_PDAYS)

    return date.replace(tzinfo=datetime.timezone.utc)


def _str2date(str_, lt, gt, period_end=False, ignore_now=False):
    """
    Converts a string to a UTC datetime.datetime

    Arguments:
        str_ --- str: String to derive datetime data from

        lt --- datetime.datetime: Derived date should be lower than this

        gt --- datetime.datetime: Derived date should be greater than this

        period_end=False --- bool: For parsing years, determines whether the
        end of the year or start should be used.

        ignore_now=False --- bool: Whether to ignore strings that refer to
        'now' (are in NOW_EQUIVS)

    Return:
        datetime.datetime/NoneType --- The result of parsing the date string
    """
    # First check if it's only a year:
    if len(str_) == 4 and year_regex.match(str_):
        year = year_regex.match(str_).group(0)
        try:
            if period_end:
                date = datetime.datetime(int(year), 12, 31)
            else:
                date = datetime.datetime(int(year), 1, 1)
            rdate = _correct_date(date, lt, gt)
            return rdate
        except ValueError:
            return None
    elif iso_datetime_pattern.match(str_):
        # Increase performance, by catching this before passing to dateparser
        try:
            date = datetime.datetime.fromisoformat(str_.strip('Z'))
            rdate = _correct_date(date, lt, gt)
        except ValueError:
            # If a too high day number for the month is used, it's bullshit
            rdate = None
        return rdate
    elif (not ignore_now) and str_.lower().strip() in NOW_EQUIVS:
        date = _now_date()
        return date
    elif fr_date_format_regex.match(str_) is not None:
        if len(str_) > 5:
            str_ = str_[5:]
    elif unseperated_date_pattern.match(str_):
        try:
            date = datetime.datetime.strptime(str_, '%Y%m%d')
            rdate = _correct_date(date, lt, gt)
            return rdate
        except ValueError:
            pass

    # Otherwise use dateparser:
    if period_end:
        try:
            date = dparser_end.get_date_data(
                str_,
                date_formats=PARSE_DATE_FORMATS)['date_obj']
        except OverflowError:
            return None
    else:
        try:
            date = dparser_begin.get_date_data(
                str_,
                date_formats=PARSE_DATE_FORMATS)['date_obj']
        except OverflowError:
            return None

    final_date = _correct_date(date, lt, gt)

    return final_date


def _convert_date_string(str_, lt, gt):
    """
    Convert string data into a date, if it is in the range of the 'lower
    than' and 'greter than' parameters
    """
    date = _str2date(str_, lt, gt, False)

    if date is not None:
        return date.strftime(st.DATE_FORMAT)
    else:
        return None


def _parse_ISO_duration(str_):
    """
    Parse a simple ISO duration to timedelta object (with one date or time
    value)
    """
    match = duration_regex.match(str_)
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


def _get_preferred_language_value(list_):
    """
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
    for lkey in trl_rules['_general']['language_keys']:
        if lkey in list_[0]:
            language_key = lkey
            break
    else:
        # Not a language alternatives list
        return

    value_key = None
    for vkey in trl_rules['_general']['language_value_keys']:
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


def _parse_timestamp(int_, lt, gt):
    """
    Convert timestamp to datetime
    """
    date = None

    if len(str(int_)) > 10:
        # Likely milisecond version, convert to seconds
        int_ = int_ / 1000

    if int_ > 86400 and int_ < 9999999999:
        date = datetime.datetime.fromtimestamp(int_)

    date = _correct_date(date, lt, gt)

    return date


def _get_value(dict_, keys, value_type=None):
    """
    Extract the first availble value from dict_, for the given keys (array). If
    value_type is given, the first value of this type is returned.
    Return str/NoneType
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

    Add. Arguments:
        schema -- JSON Schema definition
    """
    def __init__(self, *args, schema: dict = None, **kwargs):
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

    Add. Arguments:
        schema -- JSON Schema definition
    """
    def __init__(self, *args, schema: dict = None, **kwargs):
        if schema is not None:
            # If this is used in combination with the SchemaValidationMixin,
            # the schema variable may already have been set by that one
            self._schema = schema
        super().__init__(*args, **kwargs)
        self.min_str_length, self.max_str_length = self._get_min_max_length(
            self._schema
        )

    def _get_min_max_length(self, schema):
        """Return the min/max length values from the given schema"""
        min_length = schema.get('minLength', 0)
        max_length = schema.get('maxLength', 9999999)
        return min_length, max_length

    def truncate_string(self, str_, subkey: str = None):
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

        return _aux.filter_truncate_string(
            str_, min_length, max_length
        )


class DatePreparser(Preparser):
    """
    Preparses dictionaries that can contain data for one of the date fields
    """
    def __init__(
            self, fields: list[str], *, type_translator_mapping: dict,
            datetype_keys: list[str], datevalue_keys: list[str]
            ):
        super().__init__(fields)
        self.type_translator_mapping = type_translator_mapping
        self.datetype_keys = datetype_keys
        self.datevalue_keys = datevalue_keys

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
                if isinstance(data, str):
                    org_typename = data.lower()
                elif isinstance(data, dict):
                    org_typename = data.get(st.REP_TEXTKEY)
                else:
                    continue

                if org_typename is not None and \
                        org_typename in self.type_translator_mapping:
                    translator_name = \
                        self.type_translator_mapping[org_typename]
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
                            preparsing_results['timePeriod'] = {
                                'temporal': data
                            }
                            return
                    date = _str2date(data, max_date, min_date)
                    if date is not None:
                        break
                elif isinstance(data, int):
                    date = _parse_timestamp(data, max_date, min_date)
                    if date is not None:
                        break
                elif isinstance(data, dict) and st.REP_TEXTKEY in data:
                    date = _str2date(data[st.REP_TEXTKEY], max_date, min_date)
                    if date is not None:
                        break
        else:
            return False

        def_key = '_preparsed_date'
        if translator_name == 'OtherDateTranslator':
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

    def _process_list(self, list_):
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

    def __init__(
            self, fields: list[str], *, schema: dict,
            dict_key_priority: list[str], type_keys: list[str],
            type_priority: list[str]
            ):
        super().__init__(fields, schema=schema)
        self.dict_key_priority = dict_key_priority
        self.type_keys = type_keys
        self.type_priority = type_priority

    def _process_string(self, str_):
        if str_.lower() in NONE_STRINGS or str_.lower() == 'description' or\
                str_.lower() == 'abstract':
            return None
        desc = _convert_if_html(str_)
        desc = md_links_cregex.sub(r'\1', desc)
        desc = manylines_cregex.sub('\n\n', desc)
        desc = desc.strip()
        return self.truncate_string(desc)

    def _process_dict(self, dict_):
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

    def _process_list(self, list_):
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

    def _process(self, payload):
        """
        Currently this only supports string values, and mapping them to the
        'value' property
        """
        if isinstance(payload, str):
            if payload.lower() not in NONE_STRINGS:
                version_data = {
                    'value': payload
                }
                if self.is_valid(version_data):
                    return version_data


class CreatorTranslator(SchemaValidationMixin, FieldTranslator):
    """Field translator for the 'creator' field"""
    field_name = "creator"

    def _split_creators(self, str_):
        # For now, only split authors if the string contains multi & or ;
        if str_.count(';') > 1:
            return str_.split(';')
        elif str_.count('&') > 1:
            return str_.split('&')
        else:
            return [str_]

    def _process_string(self, str_):
        if str_.lower() in NONE_STRINGS or email_adress_regex.match(str_) or \
                '{' in str_:
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
                            initials_regex.search(first_name):
                        c_str = '{} {}'.format(first_name, last_name)
            # If there is a number in brackets, remove it (for figshare)
            c_str = bracketed_numbers_regex.sub('', c_str).strip()

            creators.append({'name': c_str})

        return creators if creators != [] else None

    def _process_dict(self, dict_):
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

    def _process_list(self, list_):
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

    def _process(self, payload):
        result = super()._process(payload)
        if result is not None and len(result) <= self._schema['maxLength']:
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
        super().__init__(fields, schema=schema)
        self.dict_key_priority = dict_key_priority
        self.url_keys = url_keys

    def _process_string(self, str_) -> dict:
        str_ = str_.strip()
        if self.is_valid(str_, subkey='name') and str_ not in NONE_STRINGS \
                and not email_adress_regex.match(str_) \
                and not url_regex.match(str_):
            for string_start in IGNORE_STARTSWITH:
                if str_.lower().startswith(string_start):
                    break
            else:
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
                    is_url = url_regex.match(data)
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

    def _process_list(self, list_):
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

    Additional arguments:
        lt -- Detected dates should be lower than this (Either the string
        'now' or a datetime object)

        gt -- Detected dates should be greater than this (Either the string
        'now' or a datetime object)

        favor_earliest -- If True, it tries to extract the earliest date from
        the data, if False (default), if tries to extract the latest date
    """
    def __init__(
            self, fields: list[str], *, lt: Union[str, datetime.datetime],
            gt: Union[str, datetime.datetime], favor_earliest: bool = False
            ):
        super().__init__(fields)
        self.lt = _parse_date_requirement(lt)
        self.gt = _parse_date_requirement(gt)
        self.favor_earliest = favor_earliest

    def _process_string(self, str_):
        return _convert_date_string(str_, self.lt, self.gt)

    def _process_int(self, int_):
        dt = _parse_timestamp(int_, self.lt, self.gt)
        if dt is not None:
            return dt.strftime(st.DATE_FORMAT)

    def _get_dict_payload_str(self, dict_):
        if st.REP_TEXTKEY in dict_:
            payload = dict_[st.REP_TEXTKEY]
            if isinstance(payload, str):
                return payload

    def _get_list_payload_str(self, list_):
        if len(list_) > 0:
            # As a simplification, only the first item is checked
            payload = list_[0]
            if isinstance(payload, str):
                return payload

    def _process(self, payload):
        """
        This intermediate step is not used by this translator
        """
        pass

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """
        Note that this skips the _process function, since some tests on the
        data itself need to be performed at this level
        """
        date = None
        inaccurate_date = None
        for field in self.fields:
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]
            if isinstance(payload, (str, dict, list)):
                if isinstance(payload, dict):
                    payload = self._get_dict_payload_str(payload)
                    if payload is None:
                        continue
                elif isinstance(payload, list):
                    payload = self._get_list_payload_str(payload)
                    if payload is None:
                        continue
                new_date = self._process_string(payload)
                if new_date is not None and len(payload) == 4 and\
                        year_regex.match(payload):
                    inaccurate_date = new_date
                    continue
            elif isinstance(payload, int):
                new_date = self._process_int(payload)
            elif isinstance(payload, datetime.datetime):
                new_date = payload.strftime(st.DATE_FORMAT)
            else:
                continue

            if new_date is not None:
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

    Adds Arguments:
        type_mapping -- This contains all items of the fields parameters
        as keys, and as values the types that they are mapped to
    """
    field_name = 'otherDates'

    def __init__(self, *args, type_mapping: dict, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_mapping = type_mapping

    def translate(self, metadata: ResourceMetadata, **kwargs):
        """
        Override DateTranslator 'translate' function, to apply the proper
        data format for otherdates
        """
        otherdates = []
        for field in self.fields:
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]
            date_type = self.type_mapping[field]
            is_accurate = True
            if isinstance(payload, (str, dict, list)):
                if isinstance(payload, dict):
                    payload = self._get_dict_payload_str(payload)
                    if payload is None:
                        continue
                elif isinstance(payload, list):
                    payload = self._get_list_payload_str(payload)
                    if payload is None:
                        continue
                new_date = self._process_string(payload)
                if new_date is not None and len(payload) == 4 and\
                        year_regex.match(payload):
                    is_accurate = False
            elif isinstance(payload, int):
                new_date = self._process_int(payload)
            elif isinstance(payload, datetime.datetime):
                new_date = payload.strftime(st.DATE_FORMAT)
            else:
                continue

            if new_date is not None:
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


# Classes are reordered, so the __init__function of the FieldTranslator is
# skipped, because the fields parameter for this one has a different format
class ContactTranslator(FieldTranslator, SchemaValidationMixin):
    """
    Translator for the 'contact' field

    Specific arguments:
        fields -- Each field, with an array of strings indicating whether the
        field contains a 'name', or 'details', or both.

        primary_pars -- Describes pairs of fields that contain the name and
        the details. This is a list, where each item is a list of length two,
        containing both fields, starting with the one that holds the name

        dict_key_priorities -- For each subkey (name, details), and for
        details, each detailsType, an array of the dict keys related to these
        elements.
    """
    field_name = 'contact'

    def __init__(
            self, fields: dict, primary_pairs: list, dict_key_priorities: dict,
            schema: dict
            ):
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
        if (str_.lower() in NONE_STRINGS) or email_adress_regex.match(str_) or\
                url_regex.match(str_):
            return
        elif not self.is_valid(str_, 'name'):
            return
        else:
            if str_.count(',') > 1:
                str_opts = str_.split(',')
                for str_ in str_opts:
                    if name_regex.match(str_):
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
                (dtype == 'email' and not email_adress_regex.match(str_)) or
                (dtype == 'address' and not (',' in str_ or '\n' in str_)) or
                (dtype == 'phone' and phone_regex.match(str_) is None)
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

    Specific Arguments:
        dict_key_mapping -- For each possible dict key, describes whether it
        contains a 'url' or 'text'

        name_starts -- If the string starts with any of the phrases in this
        list, it's considered a name rather then content
    """
    field_name = 'license'

    def __init__(
            self, *args, dict_key_mapping: dict, name_starts: list[str],
            **kwargs
            ):
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
        if not (self.is_valid(str_, 'name') or self.is_valid(str_, 'content')):
            return

        if url_regex.match(str_):
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
                    if url_regex.match(value) and\
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

    def _process_list(self, list_):
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

    Specific arguments:
        period_dict_keys -- Dictionary keys that contain possible period
        information

        period_mapping -- A mapping from field values to a period
    """
    field_name = 'maintenance'

    def __init__(
            self, *args, period_dict_keys: list[str], period_mapping: dict,
            **kwargs
            ):
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

    Specific arguments:
        dict_key_priority -- The priority of dictionary keys to process
    """
    field_name = 'identifier'

    def __init__(self, *args, dict_key_priority: list[str], **kwargs):
        super().__init__(*args, **kwargs)
        self.dict_key_priority = dict_key_priority

    def _extract_isbn(self, str_):
        match = isbn_regex.match(str_)
        if match:
            isbn = match.group(2)
            cleaned_isbn = ''.join(
                [c for c in isbn if c.isdigit() or c == 'x']
            )

            length = len(cleaned_isbn)

            if length == 10 or length == 13:
                return {'type': 'ISBN', 'value': cleaned_isbn}

    def _process_string(self, str_):
        lstr = str_.lower()
        if lstr == '':
            return None
        elif lstr.startswith('10.') or 'doi' in lstr:
            match = doi_regex.match(str_)
            data = {'type': 'DOI', 'value': match.group(7)} if match else None
        elif str_[0].isdigit() or 'isbn' in lstr:
            data = self._extract_isbn(str_)

        return data

    def _process_dict(self, dict_):
        for key in self.dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    result = self._process(dat)
                    if result is not None:
                        return result

    def _process_list(self, list_):
        for item in list_:
            result = self._process(item)
            if result is not None:
                return result


class TypeTranslator(FieldTranslator):
    field_name = 'type'

    def __init__(
            self, *args, post_mapping: dict, dict_key_priority: list[str],
            **kwargs
            ):
        super().__init__(*args, **kwargs)
        self.post_mapping = post_mapping
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
                if desc in self.post_mapping:
                    rtype = self.post_mapping[desc]

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

    def _get_full_hierarchy(self, type_) -> list:
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
    field_name = 'subject'

    def __init__(
            self, *args, source_max_size: int, dict_key_priority: list[str],
            **kwargs
            ):
        super().__init__(*args, **kwargs)
        self.source_max_size = source_max_size
        self.dict_key_priority = dict_key_priority

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

        new_sample = [topic_re.sub('', unidecode.unidecode(s)) for s
                      in new_sample]

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

    def _process_list(self, list_):
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
        if str_ in translated_subjects:
            return subject_mapping[str_]
        else:
            return []

    def _relations_removed(self, subject_set) -> list[str]:
        """
        Remove parents and relations from a set of subjects, keeps only
        lowest level unique subjects. Also removes relations of relations
        """
        relations_parents = set()
        for subject in subject_set:
            relations_parents.update(subject_scheme_data[subject]
                                     ['all_parents_relations'])

        return [s for s in subject_set if s not in relations_parents]

    def _parents_added(self, subject_list) -> list[str]:
        """
        Add all parents to the list of subjects
        """
        total_subjects = set()
        for subject in subject_list:
            total_subjects.add(subject)
            total_subjects.update(
                subject_scheme_data[subject]['all_parents_relations']
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
    field_name = 'location'

    def __init__(
            self, *args, bbox_field_pairs: list[list[str]],
            bbox_key_pairs: list[list[str]], **kwargs
            ):
        super().__init__(*args, **kwargs)
        self.bbox_field_pairs = bbox_field_pairs
        self.bbox_field_pair_sets = [set(pair) for pair in bbox_field_pairs]
        self.bbox_key_pairs = bbox_key_pairs
        self.bbox_key_pair_sets = [set(pair) for pair in bbox_key_pairs]
        self.translate_from.update(
            [field for pair in self.bbox_field_pairs for field in pair]
        )

    def _create_bbox_geometry(
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

    def _create_location(
            self, name: str = None, geometry: dict = None,
            elevation: float = None
            ):
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

    def _bbox_is_valid(
            self, xmin: float, ymin: float, xmax: float, ymax: float
            ) -> bool:
        return (
            # max greater than min (or equal, then it's created as point)
            (xmin <= xmax and ymin <= ymax) and
            # Within valid bounds
            (xmin >= -180 and xmax <= 180 and ymin >= -90 and ymax <= 90) and
            # Not all zero
            not (xmin == xmax == ymin == ymax == 0) and
            # Not the entire globe
            not (xmin == -180 and xmax == 180 and ymin == -90 and ymax == 90)
        )

    def _create_bbox_location(self, *args) -> dict:
        if self._bbox_is_valid(*args):
            geometry = self._create_bbox_geometry(*args)
            return self._create_location(geometry=geometry)

    def _locations_from_shape(self, shape: geometry.shape):
        results = []

        try:
            if 'multi' in shape.type.lower():
                items = list(shape)
                for item in items:
                    result = self._create_bbox_location(*item.bounds)
                    if result is not None:
                        results.append(result)
            else:
                result = self._create_bbox_location(*shape.bounds)
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
                    geojson_data = _aux.string_conversion(newstr)
                else:
                    geojson_data = None
            else:
                geojson_data = _aux.string_conversion(str_)

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

            loc = self._create_bbox_location(xmin, ymin, xmax, ymax)
            if loc is not None:
                return [loc]
        # CASE 3: It's a WKT String
        elif wkt_format_regex.match(str_):
            return self._process_wkt(str_)
        # CASE 4: It's a string describing a BBOX
        elif bbox_data_regex.match(str_):
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

            loc = self._create_bbox_location(xmin, ymin, xmax, ymax)
            if loc is not None:
                return [loc]
        else:
            bbox_match = bbox_key_value_pattern.match(str_.lower().strip())
            if bbox_match is not None:
                bbox_dict = {}
                for key_i, value_i in bbox_kv_groups:
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
                    for l in coords:
                        if len(l) != 2:
                            break
                    else:
                        xs = [c[0] for c in coords]
                        ys = [c[1] for c in coords]
                        xmin = min(xs)
                        ymin = min(ys)
                        xmax = max(xs)
                        ymax = max(ys)
                        loc = self._create_bbox_location(
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

                        loc = self._create_bbox_location(
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
                        loc = self._create_bbox_location(
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

                    loc = self._create_bbox_location(
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
        envelopes = [[l['geometry']['coordinates'][0][0],
                      l['geometry']['coordinates'][1][1],
                      l['geometry']['coordinates'][1][0],
                      l['geometry']['coordinates'][0][1]] for l in locations if
                     'geometry' in l and l['geometry']['type'] == 'envelope']

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
            self, xmin_data, ymin_data, xmax_data, ymax_data
            ) -> dict:
        """Create a location from the data in bbox pair fields"""
        try:
            xmin = float(xmin_data)
            ymin = float(ymin_data)
            xmax = float(xmax_data)
            ymax = float(ymax_data)
            return self._create_bbox_location(
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
    field_name = 'timePeriod'

    def __init__(
            self, *args, lt: datetime.datetime, gt: datetime.datetime,
            begin_end_field_pairs: list[list[str]], dict_key_priority: dict,
            seperators: list[str], remove_strings: list[str], **kwargs
            ):
        super().__init__(*args, **kwargs)
        self.lt = lt
        self.gt = gt
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
            ):
        if end > start and start > self.gt and end < self.lt:
            return {
                'type': 'About',
                'start': _date2str(start),
                'end': _date2str(end)
            }

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
            start_date = _str2date(start_payload, self.lt, self.gt,
                                   ignore_now=True)
            end_date = _str2date(end_payload, self.lt, self.gt,
                                 period_end=True)
        else:
            for sep in self.seperators:
                splitted = s.split(sep)
                splitted = [s.strip() for s in splitted]
                if len(splitted) == 2:
                    if (len(splitted[0]) == len(splitted[1])) or not (
                        (no_written_dates_pattern.match(splitted[0])
                         and no_written_dates_pattern.match(splitted[1]))
                            ):
                        # The lengths of the splitted parts may only differ, if
                        # There are written dates, like day names or month
                        # names in the date part, which can differ in length
                        # between start and end-date
                        start_payload = splitted[0]
                        end_payload = splitted[1]

                        start_date = _str2date(start_payload, self.lt, self.gt,
                                               ignore_now=True)
                        end_date = _str2date(end_payload, self.lt, self.gt,
                                             period_end=True)

                        if start_date is not None and end_date is not None:
                            break

            else:
                # If a start date was already found, and it doesn't end with
                # a duration, set end-date to now
                years = re.findall(r'\d{4}', s)
                parts = s.split('/')
                endswith_duration = duration_regex.match(parts[-1])
                if start_date is not None and not endswith_duration:
                    end_date = _str2date('now', self.lt, self.gt,
                                         period_end=True)
                elif endswith_duration or (years and len(years) == 1):
                    if endswith_duration:
                        start_payload = parts[0]
                        end_payload = _parse_ISO_duration(parts[-1])
                    else:
                        start_payload = s.strip('/-')
                        # Assume a single day/month/year coverage
                        end_payload = start_payload

                    start_date = _str2date(start_payload, self.lt, self.gt,
                                           ignore_now=True)

                    if start_date is None:
                        return []

                    end_date = None
                    if isinstance(end_payload, str):
                        end_date = _str2date(end_payload, self.lt, self.gt,
                                             period_end=True)
                    elif isinstance(end_payload, datetime.timedelta):
                        # In case a duration is parsed
                        end_date = start_date + end_payload

                    if end_date is None:
                        # Assume now, if no other can be found
                        end_date = _str2date(
                            'now', self.lt, self.gt, period_end=True
                        )

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
                        edge_date = _str2date(
                            payload, self.lt, self.gt, **date_kwargs
                        )
                    elif isinstance(payload, int):
                        edge_date = _parse_timestamp(
                            payload, self.lt, self.gt
                        )
                    if edge_date is not None:
                        timeperiod_data[edge] = edge_date
                        break

            if edge_date is None:
                break  # No use to continueing if one of the dates is not found
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

    def _process_start_end(self, start_data, end_data) -> dict:
        """This returns a single time_period rather than an array"""
        if not (isinstance(start_data, str) and isinstance(end_data, str)):
            return

        start_date = _str2date(start_data, self.lt, self.gt, ignore_now=True)
        end_date = _str2date(end_data, self.lt, self.gt, period_end=True)
        if start_date is None:
            return
        elif end_date is None:
            end_date = _now_date()

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

    def translate(self, metadata: ResourceMetadata, **kwargs):
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
            if field not in metadata.structured:
                continue

            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                time_periods.extend(result)

        if time_periods:
            time_periods = self._overlapping_merged(time_periods)
            metadata.translated[self.field_name] = time_periods


class FormatTranslator(FieldTranslator):
    field_name = 'format'

    def _derive_plain_extensions(self, str_) -> list[str]:
        """Derive one or more file extensions from a string"""
        data = []
        # Split by commas and slashes
        parts = re.split(r',|/', str_)

        for part in parts:
            part = part.strip()
            if 1 < len(part) < 6:
                new_part = unidecode.unidecode(
                    non_letter_regex.sub('', part)
                ).upper().strip()
                space_count = sum([l.isspace() for l in new_part])
                if 1 < len(new_part) < 5 and space_count == 0\
                        and new_part.lower() not in NONE_STRINGS:
                    data.append(new_part)
        return data

    def _process_string(self, str_) -> list[str]:
        data = []

        str_ = str_.lower().replace('zipped ', '').replace(' file', '')

        if str_ in file_format_mapping:
            data.append(file_format_mapping[str_])
        else:
            if '(' in str_:
                matches = between_brackets_regex.findall(str_)
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

    def _process(self, payload):
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
    field_name = 'language'

    def __init__(self, *args, dict_key_priority: list[str], **kwargs):
        super().__init__(*args, **kwargs)
        self.dict_key_priority = dict_key_priority

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
            parts = andor_regex.split(str_)
        else:
            # Check if there are brackets
            data_between_brackets = between_brackets_regex.findall(str_)
            if data_between_brackets != []:
                outside_brackets =\
                    [between_brackets_regex.sub('', str_).strip()]
                parts = data_between_brackets + outside_brackets
            else:
                parts = [str_]

        langs = []
        for part in parts:
            text = part.strip()
            if len(text) == 2:
                if text in two_letter_language_codes:
                    langs.append(text)
            else:
                decoded = unidecode.unidecode(text)
                if text in language_mapping:
                    langs.append(language_mapping[text])
                elif decoded in language_mapping:
                    langs.append(language_mapping[decoded])

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
                langs.extend(self._process_string(item))
            elif isinstance(item, dict):
                langs.extend(self._process_dict(item))

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


def coordinate_system(candidates):
    """
    Translate data about 'coordinateSystem' into the new metadata schema
    """
    rules = trl_rules['coordinateSystem']

    def handle_string(str_):
        """
        Converts string data into a coordinate system

        Arguments:
            str_ --- str: The string to be converted

        Returns:
            list --- The detected coordinate systems, empty if nothing is
            found (can still contain duplicates)
        """
        epsg_list = []
        str_ = str_.lower().strip()
        mentioned_codes = epsg_regex.findall(str_)
        if integer_regex.match(str_):
            # Check if the integer is a valid EPSG code
            epsg = int(str_)
            if epsg in epsg_codes:
                epsg_list.append(epsg)
        elif mentioned_codes != []:
            # Use codes that are referenced to as 'EPSG:...'
            for code in mentioned_codes:
                code = int(code)
                if code in epsg_codes:
                    epsg_list.append(code)
        elif str_.startswith('geogcs[') or str_.startswith('projcs['):
            # Parse the projection name from WKT, and convert to EPSG:
            match = cs_name_regex.match(str_)
            if match:
                name = match.group(4).lower()
                if name in name_to_epsg.keys():
                    epsg_list.append(name_to_epsg[name])
        elif str_.startswith('wgs') and '84' in str_:
            epsg_list.append(4326)
        else:
            if str_ in name_to_epsg:
                epsg_list.append(name_to_epsg[str_])

        return epsg_list

    def handle_dict(dict_):
        """
        Converts dict data into a coordinate system

        Arguments:
            str_ --- str: The dict to be converted

        Returns:
            list --- The detected coordinate systems, empty if nothing is
            found (can still contain duplicates)
        """
        epsg_list = []
        for key in rules['dict_key_priority']:
            if key in dict_:
                value = dict_[key]
                if isinstance(value, str):
                    epsg_list += handle_string(value)
                elif isinstance(value, int):
                    if value in epsg_codes:
                        epsg_list.append(value)
                if epsg_list != []:
                    break

        return epsg_list

    epsgs = []
    for key in rules['data_prority']:
        if key in candidates:
            value = candidates[key]
            if isinstance(value, str):
                epsgs += handle_string(value)
            elif isinstance(value, dict):
                epsgs += handle_dict(value)

    if epsgs == []:
        epsgs = None
    else:
        epsgs = list(set(epsgs))

    return epsgs


def program_language(candidates):
    """
    Translate data about 'programLanguage' into the new metadata schema
    """
    raise NotImplementedError


def software(candidates):
    """
    Translate data about 'software' into the new metadata schema
    """
    raise NotImplementedError


def platform(candidates):
    """
    Translate data about 'platform' into the new metadata schema
    """
    raise NotImplementedError


def device(candidates):
    """
    Translate data about 'device' into the new metadata schema
    """
    raise NotImplementedError


def quality(candidates):
    """
    Translate data about 'quality' into the new metadata schema
    """
    raise NotImplementedError


def lineage(candidates):
    """
    Translate data about 'lineage' into the new metadata schema
    """
    raise NotImplementedError


def contribution(candidates):
    """
    Translate data about 'contribution' into the new metadata schema
    """
    raise NotImplementedError


def untranslated(candidates, independent_translations):
    """
    Put untranslated metadata attributes (e.g. title, description) in the new
    metadata schema. This requires 'first_round_translations', including a
    language to work.

    Arguments:
        candidates --- dict: The candidate key/value pairs, that are to be
        included in the new metadata schema

        independent_translations --- dict: The translated metadata from the
        first round (independent) translations. This should contain a
        'language' key, with one language other than 'en'.

    Returns:
        list of dicts --- The untranslated data for the resource
    """
    rules = trl_rules['untranslated']
    allowed_keys = set(['title', 'abstractORdescription', 'subject'])

    # Get the relevant language data, used to know what untranslated data
    # to obtain
    language = independent_translations.get('language')
    if language is None:
        return None
    elif 'en' in language:
        if len(language) == 2:
            language.remove('en')
            lang = language[0]
        else:
            return None
    else:
        if len(language) == 1:
            lang = language[0]
        else:
            return None

    untransl_data = {'language': lang}

    # First check if there is a key with full translation data:
    for key in rules["full_translation_keys"]:
        if key in candidates:
            data = candidates[key]
            if lang in data:
                lang_data = data[lang]
                tkey_candidates = {}
                for key, value in lang_data.items():
                    tkeys = trl_mapping.get(key, [])
                    for tkey in tkeys:
                        if tkey in allowed_keys:
                            if tkey in tkey_candidates:
                                tkey_candidates[tkey].update({key: value})
                            else:
                                tkey_candidates[tkey] = {key: value}

                for tkey, candidates in tkey_candidates.items():
                    fname = tkey2fname(tkey)
                    tkey_value = globals()[fname](candidates)
                    if tkey_value is not None:
                        untransl_data[tkey] = tkey_value

    if len(untransl_data) > 1:
        return untransl_data

    # If nothing was found, try the other keys, and evaluate all of them:
    tkey_candidates = {}
    for key, org_key in rules['partial_translation_keys'].items():
        if key in candidates:
            data = candidates[key]
            if lang in data:
                lang_data = data[lang]
                tkeys = trl_mapping.get(org_key, [])
                for tkey in tkeys:
                    if tkey in allowed_keys:
                        if tkey in tkey_candidates:
                            tkey_candidates[tkey].update({key: lang_data})
                        else:
                            tkey_candidates[tkey] = {key: lang_data}

    for tkey, candidates in tkey_candidates.items():
        fname = tkey2fname(tkey)
        tkey_value = globals()[fname](candidates)
        if tkey_value is not None:
            untransl_data[tkey] = tkey_value

    if len(untransl_data) > 1:
        return untransl_data
    else:
        return None


def external_reference(candidates):
    """
    Translate data about 'externalReference' into the new metadata schema. This
    is merely a passthrough function. The full external reference data was
    already generated in the structuring stage
    """
    rules = trl_rules['externalReference']

    key = rules['passthrough'][0]

    return candidates[key]


def relation(candidates):
    """
    Translate 'relation' data into the new metadata schema
    """
    rules = trl_rules['relation']
    std_names = rules['relation_type_std_names']

    max_nr_relations = rules['length']['max']

    class Relation(dict):
        """
        A class that describes individual relations
        """

        def __init__(self, rel_type, identifier, id_type, rel_name=None):
            """
            Creates a relation object/dict. In case of empty relation name, it
            is based on the type
            """
            if rel_name is None:
                rel_name = std_names[rel_type]

            self['name'] = rel_name
            self['type'] = rel_type
            self['identifier'] = identifier
            self['identifierType'] = id_type

    def handle_dict(dict_):
        """
        Translates relatedIdentifier dict data into a relation
        """
        r_id_type = dict_.get('relatedIdentifierType')
        rtype = dict_.get('relationType')
        payload = dict_.get(st.REP_TEXTKEY)

        if not isinstance(rtype, str) or not isinstance(payload, str)\
                or rtype not in std_names:
            return None

        if r_id_type.lower() == 'doi':
            if not doi_regex.match(payload):
                return None

            if payload.startswith('10.'):
                payload = 'https://doi.org/' + payload
            elif not payload.startswith('http'):
                return None
        elif r_id_type.lower() == 'url':
            if not url_regex.match(payload):
                return None
        else:
            return None

        result = dict(Relation(rtype, payload, 'ExternalURL'))

        return result

    def handle_list(list_):
        """
        Translates lists of relatedIdentifiers into the relation format
        """
        rels = []
        for item in list_:
            if isinstance(item, dict):
                result = handle_dict(item)
                if result is not None:
                    rels.append(result)

        return rels

    relations = []
    for key, relation_type in rules['key_relation_types'].items():
        dat = candidates.get(key)
        if dat is not None and isinstance(dat, str):
            url_data = url_regex.match(dat)
            if url_data:
                relation = dict(Relation(relation_type, dat, 'ExternalURL'))
                relations.append(relation)

    if 'relatedIdentifier' in candidates:
        dat = candidates['relatedIdentifier']
        if isinstance(dat, dict):
            result = handle_dict(dat)
            if result is not None:
                relations.append(result)
        elif isinstance(dat, list):
            relations += handle_list(dat)

    if len(relations) > max_nr_relations or relations == []:
        relations = None

    return relations


def single_entry(flattened_data, data_platform_id):
    """
    Translates one structured resource description into the new metadata format
    and determines the id of the dataset

    Input:
        flattened_data --- dict: The flattened metadata of a single resource
        description

        data_platform_id --- str: The id of the data platform, that is
        prepended to the id used internally

    Returns:
        result --- The resource description, coverted to the correct
        metadata format
    """
    # Add the id to the translated entry:
    org_uid = flattened_data['_dplatform_uid']

    # Check org_uid lenght. If too long, it gets hashed (Due to ES limitations)
    encoded_id = org_uid.encode('utf8')
    if len(encoded_id) > 256:
        org_uid = 'MD5Hash-' + hashlib.md5(encoded_id).hexdigest()

    translated_data = {
        'id': '{}-{}'.format(data_platform_id, org_uid),
        '_source_id': data_platform_id
    }

    # First devide all key's data over the translation functions:
    tkey_candidates = {}
    for key, payload in flattened_data.items():
        if payload is not None:
            tkeys = trl_mapping.get(key)
            if tkeys is None:
                continue
            for tkey in tkeys:
                if tkey in tkey_candidates:
                    tkey_candidates[tkey][key] = payload
                else:
                    tkey_candidates[tkey] = {
                        key: payload
                    }

    # Add data from pre-parsers:
    pp_keys = [k for k in tkey_candidates.keys() if '_preparser' in k]
    pre_parsing_candidates = {k: tkey_candidates.pop(k) for k in pp_keys}
    for pp_fname, candidates in pre_parsing_candidates.items():
        # # PERFORMANCE TESTING ######################################
        # if pp_fname not in function_time:
        #     function_time[pp_fname] = 0
        # start_time = time.perf_counter()
        # ############################################################
        pp_function = globals()[pp_fname]
        result = pp_function(candidates)
        for tkey, candidates in result.items():
            if tkey in tkey_candidates:
                tkey_candidates[tkey].update(candidates)
            else:
                tkey_candidates[tkey] = candidates
        # # PERFORMANCE TESTING ######################################
        # end_time = time.perf_counter()
        # function_time[pp_fname] += end_time - start_time
        # ############################################################

    # Now use the mapped data to run all independent (1st round) translations:
    for tkey, candidates in tkey_candidates.items():
        fname = tkey2fname(tkey)
        # # PERFORMANCE TESTING ######################################
        # if fname not in function_time:
        #     function_time[fname] = 0
        # start_time = time.perf_counter()
        # ############################################################
        if fname in SECOND_ROUND_FUNCTIONS:
            continue
        translation_function = globals()[fname]
        result = translation_function(candidates)
        if result is not None:
            translated_data[tkey] = result
        # # PERFORMANCE TESTING ######################################
        # end_time = time.perf_counter()
        # function_time[fname] += end_time - start_time
        # ############################################################

    # Run the dependent (2nd round) translations, if any:
    for tkey, candidates in tkey_candidates.items():
        fname = tkey2fname(tkey)
        # # PERFORMANCE TESTING ######################################
        # if fname not in function_time:
        #     function_time[fname] = 0
        # start_time = time.perf_counter()
        # ############################################################
        if fname not in SECOND_ROUND_FUNCTIONS:
            continue
        translation_function = globals()[fname]
        result = translation_function(candidates, translated_data)
        if result is not None:
            translated_data[tkey] = result
        # # PERFORMANCE TESTING ######################################
        # end_time = time.perf_counter()
        # function_time[fname] += end_time - start_time
        # ############################################################

    return translated_data
