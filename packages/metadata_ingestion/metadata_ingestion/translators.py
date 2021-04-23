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

    def __init__(self, fields, **kwargs):
        if self.has_circular_dependencies():
            raise TypeError(
                'Dependencies of {} are circular'.format(
                    self.__class__.__name__
                )
            )
        self.fields = fields
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

    @abstractmethod
    def translate(metadata: ResourceMetadata, preparsed_data: dict = None):
        """
        Translate the metadata, by using the contents of metadata.structured as
        source, and storing the results in metadata.translated. Any data from
        pre-parsers is passed as 'parsed_data'.
        """
        pass


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


class SchemaValidationMixin:
    """
    Mixin adds the .validate function to a class, based on the 'schema' kwargs
    """
    def __init__(self, *args, schema, **kwargs):
        super().__init__(*args, **kwargs)
        self.validate = fastjsonschema.compile(schema)


class StringTruncationMixin:
    """
    Mixin that adds the .truncate_string function, based on the schema
    """
    def __init__(self, *args, schema, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_str_length = schema.get('minLength', 0)
        self.max_str_length = schema.get('maxLength', 9999999)

    def truncate_string(self, str_):
        """
        Truncate or filter a string. If the length of the string is less than
        the minimum length, None is returned, if it's above the maximum length,
        a truncated version is returned
        """
        return _aux.filter_truncate_string(
            str_, self.min_str_length, self.max_str_length
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

    def _process(self, payload):
        if isinstance(payload, str):
            return self._process_string(payload)
        elif isinstance(payload, dict):
            return self._process_dict(payload)
        elif isinstance(payload, list):
            return self._process_list(payload)

    def translate(self, metadata: ResourceMetadata, preparsed_data=None):
        for field in self.fields:
            if field not in metadata.structured:
                continue
            payload = metadata.structured[field]
            result = self._process(payload)
            if result is not None:
                metadata.translated[self.field_name] = result

            return


def abstractORdescription(candidates):
    """
    Generate abstract or description from a dict of input key/value candidates
    """
    rules = trl_rules['abstractORdescription']

    def handle_string(str_):
        """
        Convert a candidate string value to the correct description format
        """
        if str_.lower() in NONE_STRINGS or str_.lower() == 'description' or\
                str_.lower() == 'abstract':
            return None
        desc = _convert_if_html(str_)
        desc = md_links_cregex.sub(r'\1', desc)
        desc = manylines_cregex.sub('\n\n', desc)
        desc = desc.strip()
        desc = _aux.filter_truncate_string(desc,
                                           rules['length']['min'],
                                           rules['length']['max'])

        return desc

    def handle_dict(dict_):
        """
        Convert a candidate dict value to the correct description format
        """
        desc = None
        if 'PT_FreeText' in dict_:
            # For GMD format language alternatives
            language_options = dict_['PT_FreeText']
            if isinstance(language_options, list):
                langval = _get_preferred_language_value(language_options)
                if langval is not None:
                    desc = handle_string(langval)
                    if desc is not None:
                        return desc

        for key in rules['dict_key_priority']:
            value = dict_.pop(key, None)
            if value is not None and isinstance(value, str):
                desc = handle_string(value)
                if desc is not None:
                    break
        else:
            for key, value in dict_.items():
                if key not in rules['dict_keys_type'] and\
                        isinstance(value, str):
                    desc = handle_string(value)
                    if desc is not None:
                        break

        return desc

    def handle_list(list_):
        """"
        Convert a candidate list value to the correct description format
        """
        # First try to see of it's a list of language alternatives
        langval = _get_preferred_language_value(list_)
        if langval is not None:
            desc = handle_string(langval)
            if desc is not None:
                return desc

        # Extract all candidates, and priority based on type
        extracted_descs_prios = []
        found_prio = False
        for item in list_:
            c_desc_prio = 99999
            c_desc = None
            if isinstance(item, str):
                c_desc = handle_string(item)
            elif isinstance(item, dict):
                c_desc = handle_dict(item)
                c_desc_type = _get_value(
                    item, rules['dict_keys_type'], value_type=str
                )
                if c_desc_type in rules['type_priority']:
                    c_desc_prio = rules['type_priority'].index(c_desc_type)
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

    def convert_description_data(candidate):
        """
        Convert the candidate to the correct description format
        """
        desc = None
        if isinstance(candidate, str):
            desc = handle_string(candidate)
        elif isinstance(candidate, dict):
            desc = handle_dict(candidate)
        elif isinstance(candidate, list):
            desc = handle_list(candidate)

        return desc

    desc = None
    for ckey in rules['data_priority']:
        candidate = candidates.pop(ckey, None)
        desc = convert_description_data(candidate)
        if desc is not None:
            break
    else:
        for ckey, candidate in candidates.items():
            desc = convert_description_data(candidate)
            if desc is not None:
                break

    return desc


def version(candidates):
    """
    Translate version information based on the given candidate key/value
    combinations

    Note: Sub-key 'notes' not yet implemented, given lack of occurence
    """
    version = {}
    rules = trl_rules['version']

    def convert_version_value(candidates):
        """
        Convert given candidates to version_value, and remove item used to
        determine it
        """
        value = None
        sub_rules = rules['children']['value']
        max_lenght = sub_rules['length']['max']
        for key in sub_rules['data_priority']:
            if key in candidates:
                data = candidates[key]
                if isinstance(data, str) and data.lower() not in NONE_STRINGS\
                        and len(data) <= max_lenght:
                    value = data
                    break

        return value

    version_value = convert_version_value(candidates)
    if version_value is not None:
        version['value'] = version_value

    version = version if version != {} else None

    return version


def status(candidates):
    """
    Translate resource status information
    """
    raise NotImplementedError


def creator(candidates):
    """
    Translate creator information
    """
    rules = trl_rules['creator']
    creator = []

    name_len_min = rules['children']['name']['length']['min']
    name_len_max = rules['children']['name']['length']['max']

    org_len_min = rules['children']['organization']['length']['min']
    org_len_max = rules['children']['organization']['length']['max']

    aff_len_min = rules['children']['affiliation']['length']['min']
    aff_len_max = rules['children']['affiliation']['length']['max']

    id_schemes = set(rules['children']['identifierScheme']['ctrl_vocab'])

    def split_creators(candidate_value):
        """
        Splits the authors in case of multiple authors
        """
        if isinstance(candidate_value, str):
            # For now, only split authors if the string contains multi & or ;
            if candidate_value.count(';') > 1:
                split_creators = candidate_value.split(';')
            elif candidate_value.count('&') > 1:
                split_creators = candidate_value.split('&')
            else:
                split_creators = [candidate_value]
        elif isinstance(candidate_value, dict):
            split_creators = [candidate_value]
        else:
            split_creators = candidate_value

        return split_creators

    def convert_string(key, str_):
        """
        Check if the string is an organization or person, and if the string
        meets the requirements.
        """
        if str_.lower() in NONE_STRINGS:
            return None
        elif email_adress_regex.match(str_):
            return None
        elif '{' in str_:
            return None

        # First check if its a creator or organization:
        creator_data = None
        if key != 'organization':
            # Treat data as a single person's name
            if name_len_min <= len(str_) <= name_len_max and ' ' in str_:
                # If last name comes first, reverse order
                if str_.count(',') == 1:
                    names = [s.strip() for s in str_.split(',')]
                    last_name = names[0]
                    first_name = names[1]
                    if last_name.count(' ') <= 1:
                        if first_name.count(' ') == 0:
                            str_ = '{} {}'.format(first_name, last_name)
                        elif first_name.count(' ') == 1 and\
                                initials_regex.search(first_name):
                            str_ = '{} {}'.format(first_name, last_name)
                # If there is a number in brackets, remove it (for figshare)
                str_ = bracketed_numbers_regex.sub('', str_).strip()

                creator_data = {
                    'name': str_
                }
        else:
            # Treat data as an organization name
            if org_len_min <= len(str_) <= org_len_max:
                creator_data = {
                    'organization': str_
                }

        return creator_data

    def affiliation_from_string(str_):
        """
        Verify whether the input is a valid affiliation. If this is true, input
        is returned, otherwise 'None' is returned.
        """
        if aff_len_min <= len(str_) <= aff_len_max:
            return str_
        else:
            return None

    def convert_dict(key, dict_):
        """
        Derive the relevant information from the dictionary data
        """
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
                        base_data = convert_string('organization', name)
                elif isinstance(type, str):
                    if 'organization' in type.lower():
                        base_data = convert_string('organization', name)
                    else:
                        base_data = convert_string('creator', name)
                else:
                    base_data = convert_string('creator', name)

                if base_data is None:
                    return None

                if 'affiliation' in dict_:
                    aff = dict_['affiliation']
                    if isinstance(aff, str):
                        affiliation = affiliation_from_string(aff)
                        if affiliation is not None:
                            base_data['affiliation'] = affiliation

                return base_data

        elif 'Name' in dict_ or 'Organisation' in dict_:
            if 'Name' in dict_:
                name = dict_['Name']
                ntype = 'creator'
            else:
                name = dict_['Organisation']
                ntype = 'organization'

            role = dict_.get('Role')
            if isinstance(role, str):
                std_role = _INSPIRE_role2type(role)
                if not std_role == 'creator':
                    return None

            if isinstance(name, str):
                return convert_string(ntype, name)

        elif 'givenName' in dict_ and 'familyName' in dict_:
            name = dict_['givenName']
            fname = dict_['familyName']
            if not (isinstance(name, str) and isinstance(fname, str)):
                return None

            base_data = convert_string('creator', f"{name} {fname}")

            if base_data is None:
                return None

            if 'affiliation' in dict_:
                aff = dict_['affiliation']
                if isinstance(aff, str):
                    affiliation = affiliation_from_string(aff)
                    if affiliation is not None:
                        base_data['affiliation'] = affiliation

            return base_data

        elif 'creatorName' in dict_ or 'authorName' in dict_:
            if 'creatorName' in dict_:
                name = dict_['creatorName']
            else:
                name = dict_['authorName']
            base_data = None
            if isinstance(name, str):
                if name.count(',') == 1:
                    last_first = name.split(',')
                    name = ' '.join([n.strip() for n in reversed(last_first)])
                base_data = convert_string('creator', name)

            if base_data is None:
                return None

            aff = None
            if 'affiliation' in dict_:
                aff = dict_['affiliation']
            elif 'authorAffiliation' in dict_:
                aff = dict_['authorAffiliation']
            if isinstance(aff, str):
                affiliation = affiliation_from_string(aff)
                if affiliation is not None:
                    base_data['affiliation'] = affiliation

            if 'nameIdentifier' in dict_:
                id_data = dict_['nameIdentifier']
                if isinstance(id_data, dict):
                    # Check completeness:
                    identifier_scheme = None
                    id_scheme = id_data.get('nameIdentifierScheme')
                    if isinstance(id_scheme, str) and id_scheme in id_schemes:
                        identifier_scheme = id_scheme

                    if identifier_scheme is not None:
                        idfr = id_data.get(st.REP_TEXTKEY)
                        if isinstance(idfr, str) and\
                                orchid_isni_regex.match(idfr):
                            identifier = idfr.replace('-', '')
                            base_data['identifierScheme'] = identifier_scheme
                            base_data['identifier'] = identifier

            return base_data

        return None

    # First try to get author information:
    for key in rules['data_priority']:
        if key in candidates:
            creator = []
            value = candidates[key]
            creator_data_list = split_creators(value)
            for creator_data in creator_data_list:
                if isinstance(creator_data, str):
                    entry_data = convert_string(key, creator_data)
                elif isinstance(creator_data, dict):
                    entry_data = convert_dict(key, creator_data)

                if entry_data is not None:
                    creator.append(entry_data)
            if len(creator) > 0:
                break
    else:
        return None

    if len(creator) > rules['length']['max']:
        creator = creator[:rules['length']['max']]

    return creator


def publisher(candidates):
    """
    Translates publisher data into the new metadata format
    """
    rules = trl_rules['publisher']
    data_priority = rules['data_priority']

    name_key_priority = rules['children']['name']['dict_key_priority']
    n_len_min = rules['children']['name']['length']['min']
    n_len_max = rules['children']['name']['length']['max']

    id_key_priority = rules['children']['identifier']['url_dict_keys']
    id_len_min = rules['children']['identifier']['length']['min']
    id_len_max = rules['children']['identifier']['length']['max']

    def process_name_string(str_):
        """
        Process a name string to check validity
        """
        data = None
        if n_len_min <= len(str_) <= n_len_max and\
                str_.lower() not in NONE_STRINGS and\
                email_adress_regex.match(str_) is None\
                and url_regex.match(str_) is None:
            for string_start in IGNORE_STARTSWITH:
                if str_.lower().startswith(string_start):
                    break
            else:
                data = str_.strip()

        return data

    def convert_string(key, str_):
        """
        Converts string data to the publisher data scheme
        """
        name = process_name_string(str_)
        if name is not None:
            return {'name': name}
        else:
            return None

    def convert_dict(key, dict_):
        """
        Converts dict data to the publisher data scheme
        """
        pub = None

        for key in name_key_priority:
            data = dict_.get(key)
            if isinstance(data, str):
                name = process_name_string(data)
                if name:
                    pub = {'name': name}
                    break
            elif isinstance(data, dict) and 'en' in data:
                name = process_name_string(data['en'])
                if name:
                    pub = {'name': name}
                    break

        if pub is not None:
            for key in id_key_priority:
                data = dict_.get(key)
                if isinstance(data, str):
                    is_url = url_regex.match(data)
                    if is_url and id_len_min <= len(data) <= id_len_max:
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

    def convert_list(key, list_):
        """
        Converts list data to the publisher data scheme
        """
        # First try to see of it's a list of language alternatives
        langval = _get_preferred_language_value(list_)
        if langval is not None:
            data = convert_string(None, langval)  # Pass dummy for 'key'
            if data is not None:
                return data

        # Otherwise try the  conventional processing method
        data = None
        for item in list_:
            if isinstance(item, dict):
                data = convert_dict(key, item)
            elif isinstance(item, str):
                data = convert_string(key, item)
            if data is not None:
                break

        return data

    for key in data_priority:
        if key in candidates:
            value = candidates[key]

            if isinstance(value, str):
                data = convert_string(key, value)
            elif isinstance(value, dict):
                data = convert_dict(key, value)
            elif isinstance(value, list):
                data = convert_list(key, value)

            if data is not None:
                break

    return data


def published_in(candidates):
    """
    Convert journal/book information to the new metadata schema
    """
    data = None
    rules = trl_rules['publishedIn']
    data_priority = rules['data_priority']

    name_contains = rules['children']['name']['contains']
    name_minlen = rules['children']['name']['length']['min']
    name_maxlen = rules['children']['name']['length']['max']

    vol_gt = rules['children']['volume']['gt']
    vol_lt = rules['children']['volume']['lt']

    issue_gt = rules['children']['issue']['gt']
    issue_lt = rules['children']['issue']['lt']

    def validate_entry(entry):
        """
        Validates and cleans an entry (converts list used for 'pages')
        """
        # Validate name:
        if not ('name' in entry and entry['name'].lower() not in NONE_STRINGS
                and name_minlen <= len(entry['name']) <= name_maxlen):
            return False

        if 'volume' in entry:
            if not vol_gt <= entry['volume'] <= vol_lt:
                entry.pop('volume')

        if 'issue' in entry:
            if not issue_gt <= entry['issue'] <= issue_lt:
                entry.pop('issue')

        if 'pages' in entry:
            pages = entry['pages']
            if pages[1] > pages[0]:
                entry['pages'] = '{}-{}'.format(pages[0], pages[1])
            else:
                entry.pop('pages')

        return True

    def convert_standardised_data_string(str_):
        """
        Converts a standardised string with TITLE=, VOLUME= etc. to the correct
        format
        """
        # Detect title:
        title_match = title_data_regex.search(str_)
        if title_match is not None:
            data = {'name': title_match.group(1)}
        else:
            return None

        volume_match = volume_data_regex.search(str_)
        if volume_match is not None:
            data['volume'] = int(volume_match.group(1))

        issue_match = issue_data_regex.search(str_)
        if issue_match is not None:
            data['issue'] = int(issue_match.group(1))

        frompage_match = frompage_data_regex.search(str_)
        untilpage_match = untilpage_data_regex.search(str_)
        if frompage_match is not None and untilpage_match is not None:
            data['pages'] = [int(frompage_match.group(1)),
                             int(untilpage_match.group(1))]

        issn_match = issn_data_regex.search(str_)
        if issn_match is not None:
            data['identifier'] = issn_match.group(1)
            data['identifierType'] = 'ISSN'

        return data

    def convert_journal_info_string(str_):
        """
        Convert a string with journal information
        """
        splitted = [s.strip() for s in
                    re.split(r",|\:(?=\s)|\;|'|\s(?=\()", str_)]
        name_detected = False
        added = set()
        data = {}
        dist_from_name = 0
        for splitted_text in splitted:
            if name_detected:
                dist_from_name += 1
                if dist_from_name > 5:
                    break
                # Detect volume:
                if 'volume' not in added:
                    vol_match = journal_volume_regex.match(
                        splitted_text.lower()
                    )
                    if vol_match:
                        vol = int(vol_match.group(1))
                        data['volume'] = vol
                        added.add('volume')
                if 'issue' not in added:
                    issue_match = journal_issue_regex.match(
                        splitted_text.lower()
                    )
                    if issue_match:
                        issue = int(issue_match.group(1))
                        data['issue'] = issue
                        added.add('issue')
                if 'pages' not in added:
                    pages_match = journal_pages_regex.match(
                        splitted_text.lower()
                    )
                    if pages_match:
                        from_ = int(pages_match.group(2))
                        until = int(pages_match.group(3))
                        pages = [from_, until]
                        data['pages'] = pages
                        added.add('pages')
                if 'issn' not in added and\
                        'issn' in splitted_text.lower():
                    issn_match = journal_issn_end_regex.match(
                        splitted_text.lower()
                    )
                    if issn_match is not None:
                        data['identifier'] = issn_match.group(1)
                        data['identifierType'] = 'ISSN'
                        added.add('issn')

            else:
                for word in name_contains:
                    if word in splitted_text.lower():
                        data = {
                            'name': splitted_text,
                            'type': 'journal'
                        }
                        name_detected = True
                        break
        if name_detected:
            return data
        else:
            return None

    def convert_list(list_):
        "Retrieve journal/book information from"
        candidates = []
        for item in list_:
            if isinstance(item, str):
                if 'TITLE=' in item:
                    dat = convert_standardised_data_string(item)
                else:
                    dat = convert_journal_info_string(item)

                if dat is not None and validate_entry(dat):
                    candidates.append(dat)

        best = None
        if len(candidates) > 0:
            candidate_lens = [len(c) for c in candidates]
            max_len = max(candidate_lens)
            ind_ = candidate_lens.index(max_len)

            best = candidates[ind_]

        return best

    for key in data_priority:
        if key in candidates:
            dat = candidates[key]
            if isinstance(dat, list):
                data = convert_list(dat)
                if data is not None:
                    break

    return data


def issued(candidates):
    """
    Convert issued date information to the new metadata schema
    """
    rules = trl_rules['issued']
    lt = _parse_date_requirement(rules['lt'])
    gt = rules['gt']

    date = None
    provisional = None
    for key in candidates:
        new_date = None
        data = candidates[key]
        if isinstance(data, str):
            new_date = _convert_date_string(data, lt, gt)
            if len(data) == 4 and year_regex.match(data) and\
                    new_date is not None:
                provisional = new_date  # since this will always be lower...
                continue
        elif isinstance(data, datetime.datetime):
            new_date = data.strftime(st.DATE_FORMAT)
        elif isinstance(data, int):
            dt = _parse_timestamp(data, lt, gt)
            if dt is not None:
                new_date = dt.strftime(st.DATE_FORMAT)
        elif isinstance(data, dict):
            if st.REP_TEXTKEY in data:
                payload = data[st.REP_TEXTKEY]
                if isinstance(payload, str):
                    new_date = _convert_date_string(payload, lt, gt)
                    if len(payload) == 4 and year_regex.match(payload) and\
                            new_date is not None:
                        provisional = new_date
                        continue
        else:
            continue
        if new_date is not None:
            if date is not None:
                if new_date < date:
                    date = new_date
            else:
                date = new_date

    if date is None and provisional is not None:
        date = provisional

    return date


def modified(candidates):
    """
    Convert modified date information to the new metadata schema
    """
    rules = trl_rules['modified']
    lt = _parse_date_requirement(rules['lt'])
    gt = rules['gt']

    date = None
    for key in candidates:
        new_date = None
        data = candidates[key]
        if isinstance(data, str):
            new_date = _convert_date_string(data, lt, gt)
        elif isinstance(data, datetime.datetime):
            new_date = data.strftime(st.DATE_FORMAT)
        elif isinstance(data, int):
            dt = _parse_timestamp(data, lt, gt)
            if dt is not None:
                new_date = dt.strftime(st.DATE_FORMAT)
        elif isinstance(data, dict):
            if st.REP_TEXTKEY in data:
                payload = data[st.REP_TEXTKEY]
                if isinstance(payload, str):
                    new_date = _convert_date_string(payload, lt, gt)
        else:
            continue
        if new_date is not None:
            if date is not None:
                if new_date > date:
                    date = new_date
            else:
                date = new_date

    return date


def created(candidates):
    """
    Convert created date information to the new metadata schema
    """
    rules = trl_rules['created']
    lt = _parse_date_requirement(rules['lt'])
    gt = rules['gt']

    date = None
    new_date = None
    provisional = None
    for key in candidates:
        data = candidates[key]
        if isinstance(data, str):
            new_date = _convert_date_string(data, lt, gt)
            if len(data) == 4 and year_regex.match(data) and\
                    new_date is not None:
                provisional = new_date  # since this will always be lower...
                continue
        elif isinstance(data, datetime.datetime):
            new_date = data.strftime(st.DATE_FORMAT)
        elif isinstance(data, int):
            dt = _parse_timestamp(data, lt, gt)
            if dt is not None:
                new_date = dt.strftime(st.DATE_FORMAT)
        elif isinstance(data, list) and len(data) > 0:
            # As an approximation, just use the first entry
            str_ = data[0]
            if isinstance(str_, str):
                new_date = _convert_date_string(str_, lt, gt)
                if len(str_) == 4 and year_regex.match(str_) and\
                        new_date is not None:
                    provisional = new_date  # since this will always be lower...
                    continue
            else:
                continue
        elif isinstance(data, dict):
            if st.REP_TEXTKEY in data:
                payload = data[st.REP_TEXTKEY]
                if isinstance(payload, str):
                    new_date = _convert_date_string(payload, lt, gt)
                    if len(payload) == 4 and year_regex.match(payload) and\
                            new_date is not None:
                        provisional = new_date  # since this will always be lower...
                        continue
        else:
            continue
        if new_date is not None:
            if date is not None:
                if new_date < date:
                    date = new_date
            else:
                date = new_date

    if date is None and provisional is not None:
        date = provisional

    return date


def other_dates(candidates):
    """
    Convert information about other dates to the new metadata schema
    """
    rules = trl_rules['otherDates']
    type_mapping = rules['type_mapping']
    lt = _parse_date_requirement(rules['children']['value']['lt'])
    gt = rules['children']['value']['gt']

    odates = {}  # First in another format, so multiple can be compared...
    for key in type_mapping:
        # Go trough each known type, and if this is there, try to parse
        if key in candidates:
            dtp = type_mapping[key]
            org_val = candidates[key]
            if isinstance(org_val, str):
                date = _convert_date_string(org_val, lt, gt)
            elif isinstance(org_val, datetime.datetime):
                date = org_val
            elif isinstance(org_val, int):
                date = _parse_timestamp(org_val, lt, gt)
            else:
                continue

            if date is not None:
                s_date = date.strftime(st.DATE_FORMAT)
                if dtp in odates:
                    if date < odates[dtp]:
                        odates[dtp] = s_date
                else:
                    odates[dtp] = s_date

    # Now put the data in the correct list format
    odates_list = []
    for dtp, date in odates.items():
        odates_list.append(
            {
                'type': dtp,
                'value': date
            }
        )

    if odates_list == []:
        return None
    else:
        return odates_list


def contact(candidates):
    """
    Converts contact information into the new data format
    """
    rules = trl_rules['contact']
    name_rules = rules['children']['name']
    details_rules = rules['children']['details']

    name_min_len = name_rules['length']['min']
    name_max_len = name_rules['length']['max']

    details_min_len = details_rules['length']['min']
    details_max_len = details_rules['length']['max']

    def contains_primary_pairs(candidates):
        """
        Detects whether the entry contains a primary name/email pair
        """
        contains_p_pairs = []
        for primary_pair in rules['primary_pairs']:
            if primary_pair[1] in candidates and primary_pair[0] in candidates:
                contains_p_pairs.append(primary_pair)

        return contains_p_pairs

    def convert_name_string(str_):
        """
        Convert the input name string data
        """
        if (str_.lower() in NONE_STRINGS) or email_adress_regex.match(str_) or\
                url_regex.match(str_) or\
                not (name_min_len <= len(str_) <= name_max_len):
            return None
        else:
            if str_.count(',') > 1:
                str_opts = str_.split(',')
                for str_ in str_opts:
                    if name_regex.match(str_):
                        break
                else:
                    str_ = None
            return str_

    def convert_name_dict(dict_):
        """
        Convert the contact name data in a dict to the new metadata schema
        """
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

        for key in name_rules['dict_key_priority']:
            data = dict_.get(key)
            if isinstance(data, str):
                name = convert_name_string(data)
                if name is not None:
                    break

        return name

    def convert_details_string(str_, dtype):
        """
        Convert the input details string
        """
        if (details_min_len <= len(str_) <= details_max_len):
            if (dtype == 'email' and email_adress_regex.match(str_) is None)\
                    or (dtype == 'address' and not (',' in str_
                                                    or '\n' in str_))\
                    or (dtype == 'phone' and phone_regex.match(str_) is None):
                return None
            else:
                if dtype == 'email':
                    str_ = str_.replace('mailto:', '')
                return str_
        else:
            return None

    def convert_details_dict(dict_):
        """
        Convert a dict with contact details to the new metadata schema
        """
        details = None
        details_type = None

        # Check for email adress:
        for key in details_rules['email_dict_keys']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = convert_details_string(data, 'email')
                if details is not None:
                    details_type = 'Email'
                    return details, details_type

        # Check for phone number:
        for key in details_rules['phone_dict_keys']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = convert_details_string(data, 'phone')
                if details is not None:
                    details_type = 'Phone'
                    return details, details_type

        # Check for street address:
        for key in details_rules['address_dict_keys']:
            data = dict_.get(key)
            if isinstance(data, str):
                details = convert_details_string(data, 'address')
                if details is not None:
                    details_type = 'Address'
                    return details, details_type

        return details, details_type

    def convert_name(data):
        """
        Convert contact name information into the new metadata schema
        """
        name = None
        if isinstance(data, str):
            name = convert_name_string(data)
        elif isinstance(data, dict):
            name = convert_name_dict(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    name = convert_name_dict(item)
                    if name is not None:
                        break

        return name

    def convert_details(data):
        """
        Convert contact details information into the new metadata schema
        """
        details = None
        details_type = None
        if isinstance(data, str):
            details = convert_details_string(data, 'email')
            details_type = 'Email' if details is not None else None
        elif isinstance(data, dict):
            details, details_type = convert_details_dict(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    details, details_type = convert_details_dict(item)
                    if details is not None:
                        break

        return details, details_type

    def clean_duplicates(contacts):
        """
        Cleans entries with the same 'details'. Keeps the first entries
        """
        cleaned_contacts = []
        prev_details = set()
        for ind_, contact in enumerate(contacts):
            if contact['details'] not in prev_details:
                cleaned_contacts.append(contact)
                prev_details.add(contact['details'])

        return cleaned_contacts

    # First try the primary key combinations
    ppairs = contains_primary_pairs(candidates)
    contacts = []
    for name_key, details_key in ppairs:
        name = convert_name(candidates[name_key])
        details, detailsType = convert_details(candidates[details_key])
        if name is not None and details is not None:
            contacts.append({
                "name": name,
                "details": details,
                "detailsType": detailsType
            })

    # Now try all name and details keys seperately:
    if contacts == []:
        name = None
        details = None
        for key in rules['children']['name']['data_priority']:
            if key in candidates:
                name = convert_name(candidates[key])
                if name is not None:
                    break

        for key in rules['children']['details']['data_priority']:
            if key in candidates:
                details, detailsType = convert_details(candidates[key])
                if details is not None:
                    break

        if name is not None and details is not None:
            contacts.append({
                "name": name,
                "details": details,
                "detailsType": detailsType
            })
        else:
            contacts = None

    if contacts is not None:
        contacts = clean_duplicates(contacts)

    return contacts


def license(candidates):
    """
    Translate license information into the new metadata schema
    """
    rules = trl_rules['license']
    data_priority = rules['data_priority']
    dict_key_mapping = rules['dict_key_mapping']

    ns_name_starts = rules['children']['name']['nospace_starts_with']

    min_content_len = rules['children']['content']['length']['min']
    max_content_len = rules['children']['content']['length']['max']

    def map_text(str_):
        """
        Decide whether a string is a name, license text or neither
        """
        ttype = None
        if ' ' in str_:
            if 6 < len(str_) <= 64:
                ttype = "name"
            elif len(str_) > 64:
                ttype = "text"
        else:
            for start in ns_name_starts:
                if str_.lower().startswith(start):
                    ttype = "name"
                    break

        return ttype

    def update_missing(previous_data, new_data):
        """
        Updates the previous data with keys from the new_data, if they do not
        yet exist
        """
        if 'name' in new_data and 'name' not in previous_data:
            previous_data['name'] = new_data['name']

        if 'content' in new_data:
            if 'content' not in previous_data or\
                    (new_data['type'] == 'URL'
                     and previous_data['type'] == 'Text'):
                previous_data['content'] = new_data['content']
                previous_data['type'] = new_data['type']

    def clean_text_data(str_):
        """
        Cleans non-url text data and decides the type
        """
        data = None
        if _is_valid_string(str_, check_startswith=True,
                            check_contains=True):
            cleaned_str = _convert_if_html(str_)
            cleaned_str = cleaned_str.strip()
            ttype = map_text(cleaned_str)
            if ttype == 'name':
                data = {'name': cleaned_str}
            elif ttype == 'text':
                text = _aux.filter_truncate_string(cleaned_str, min_content_len,
                                             max_content_len)
                data = {
                    'type': 'Text',
                    'content': text
                }

        return data

    def handle_dict(dict_):
        """
        Maps the data in dict keys to the correct output format
        """
        dict_data = {}
        for key, value in dict_.items():
            if key in dict_key_mapping and isinstance(value, str):
                maps_to = dict_key_mapping[key]
                if maps_to == "url":
                    if url_regex.match(value) and \
                            min_content_len <= len(value) <= max_content_len:
                        urldata = {
                            'content': value,
                            'type': 'URL'
                        }
                        update_missing(dict_data, urldata)
                elif maps_to == "text":
                    text_data = clean_text_data(value)
                    if text_data is not None:
                        update_missing(dict_data, text_data)

                if len(dict_data) == 3 and dict_data['type'] == 'URL':
                    break

        if dict_data == {}:
            dict_data = None

        return dict_data

    def handle_string(str_):
        """
        Convert non-url text data into the new metadata format
        """
        data = None
        if url_regex.match(str_):
            if min_content_len <= len(str_) <= max_content_len:
                data = {
                    'type': 'URL',
                    'content': str_
                }
        elif not _is_valid_string(str_):
            return None
        else:
            data = clean_text_data(str_)

        return data

    def router(candidate):
        """
        Routes the data for the given candidate to the correct function
        """
        data = None
        if isinstance(candidate, str):
            data = handle_string(candidate)
        elif isinstance(candidate, list):
            data = {}
            for item in candidate:
                if isinstance(item, str):
                    dat = handle_string(item)
                    if dat is not None:
                        update_missing(data, dat)
                        if len(data) == 3 and data['type'] == 'URL':
                            break
                elif isinstance(item, dict):
                    dat = handle_dict(item)
                    if dat is not None:
                        update_missing(data, dat)
                        if len(data) == 3 and data['type'] == 'URL':
                            break
            if data == {}:
                data = None
        elif isinstance(candidate, dict):
            data = handle_dict(candidate)

        return data

    license_info = {}
    for key in data_priority:
        if key in candidates:
            data = router(candidates[key])
            if data is not None:
                update_missing(license_info, data)
                if len(license_info) == 3 and license_info['type'] == 'URL':
                    break

    if license_info == {}:
        license_info = None

    return license_info


def maintenance(candidates):
    """
    Convert information about maintenance of the data to the new metadata
    format
    """
    maintenance = None
    rules = trl_rules['maintenance']

    period_mapping = rules['period_mapping']
    period_dict_keys = rules['period_dict_keys']
    period_priority = rules['period_priority']

    def convert_frequency_string(str_):
        """
        Convert a string with information about update frequency into the new
        format
        """
        output = None
        if str_.startswith('http'):
            str_ = str_.split('/')[-1]

        str_ = str_.lower()

        if str_ in period_mapping:
            output = 'Updated {}'.format(period_mapping[str_])

        return output

    def convert_update_frequency(candidate):
        """
        Converts update frequency information to fill the maintenance entry
        """
        data = None
        if isinstance(candidate, str):
            data = convert_frequency_string(candidate)
        elif isinstance(candidate, dict):
            for key in period_dict_keys:
                if key in candidate:
                    dat = candidate[key]
                    if isinstance(dat, str):
                        data = convert_frequency_string(dat)
                        if data is not None:
                            break
        elif isinstance(candidate, list):
            for item in candidate:
                if isinstance(item, str):
                    data = convert_frequency_string(item)
                    if data is not None:
                        break

        return data

    for key in period_priority:
        if key in candidates:
            mdata = convert_update_frequency(candidates[key])
            if mdata is not None:
                candidates.pop(key)
                if maintenance is None:
                    maintenance = mdata
                else:
                    maintenance += '; ' + mdata
                break

    return maintenance


def context(candidates):
    """
    Convert context information into the new metadata schema
    """
    raise NotImplementedError


def funding(candidates):
    """
    Convert funding information into the new metadata schema
    """
    raise NotImplementedError


def identifier(candidates):
    """
    Convert identifier information into the new metadata schema
    """
    identifier = None
    rules = trl_rules['identifier']

    data_priority = rules['data_priority']
    dict_key_priority = rules['dict_key_priority']

    def extract_isbn(str_):
        """
        Extract the ISBN from a string
        """
        match = isbn_regex.match(str_)
        if match:
            isbn = match.group(2)
            cleaned_isbn = ''.join(
                [c for c in isbn if c.isdigit() or c == 'x']
            )

            length = len(cleaned_isbn)

            if length == 10 or length == 13:
                return {'type': 'ISBN', 'value': cleaned_isbn}
            else:
                return None

    def convert_string(str_):
        """
        Convert string identifier data into the new metadata format
        """
        data = None

        if str_ == '':
            return None

        lstr = str_.lower()

        if lstr.startswith('10.') or 'doi' in lstr:
            match = doi_regex.match(str_)
            data = {'type': 'DOI', 'value': match.group(7)} if match else None
        elif str_[0].isdigit() or 'isbn' in lstr:
            data = extract_isbn(str_)

        return data

    def convert_identifier(candidate):
        """
        Convert identifier information to the new metadata schema
        """
        data = None
        if isinstance(candidate, str):
            data = convert_string(candidate)
        elif isinstance(candidate, dict):
            for key in dict_key_priority:
                if key in candidate:
                    dat = candidate[key]
                    data = convert_identifier(dat)
                    if data is not None:
                        break
        elif isinstance(candidate, list):
            for item in candidate:
                data = convert_identifier(item)
                if data is not None:
                    break

        return data

    for key in data_priority:
        if key in candidates:
            identifier = convert_identifier(candidates[key])
            if identifier is not None:
                break

    return identifier


def type_(candidates):
    """
    Translate information about the type of resource described by the metadata

    returns:
        list|NoneType --- If type data is found, a list is returned, with the
        type, including any parent types
    """
    rules = trl_rules['type']
    data_priority = rules['data_priority']
    dict_key_priority = rules['dict_key_priority']
    mapping = rules['post_mapping']

    def convert_string(str_):
        """
        Convert string data about the type of the resource
        """
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
                if desc in mapping:
                    rtype = mapping[desc]

            if rtype is None and ':' in desc:
                rtype = convert_string(desc.split(':')[-1])

            if rtype is not None:
                break

        return rtype

    def handle_dict(dict_):
        """
        Try to extract type information from a dict
        """
        data = None
        for key in dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    data = convert_string(dat)
                    if data is not None:
                        break

        return data

    def convert_type(candidate):
        """
        Convert resource type information
        """
        data = None
        if isinstance(candidate, str):
            data = convert_string(candidate)
        elif isinstance(candidate, dict):
            data = handle_dict(candidate)
        elif isinstance(candidate, list):
            # An entry can only have one type in DataClopedia
            # If there is a list, it's reviewed whether 'Dataset' is in there
            # or an entry starting with 'Dataset:', then the entry will get
            # that one, otherwise it gets the first meaningfull entry
            types = [convert_type(item) for item in candidate]
            filt_types = [t for t in types if t is not None]
            if filt_types != []:
                for type_ in filt_types:
                    if type_.startswith('Dataset'):
                        return type_
                else:
                    return filt_types[0]

        return data

    type_ = None
    for key in data_priority:
        if key in candidates:
            type_ = convert_type(candidates[key])
            if type_ is not None:
                break
    else:
        for key in candidates:
            type_ = convert_type(candidates[key])
            if type_ is not None:
                break

    # Now also add parent types (if applicable) and return an array
    if type_ is not None:
        type_parts = type_.split(':')
        all_types = []
        for i in range(1, len(type_parts) + 1):
            all_types.append(':'.join(type_parts[:i]))
        type_ = all_types

    return type_


def subject(candidates):
    """
    Translate information about the subject of a resource to the new metadata
    schema
    """
    rules = trl_rules['subject']

    max_subjects = rules['array_length']['max']
    max_sarray_length = rules["source_array_length"]["max"]
    dict_key_priority = rules['dict_key_priority']
    data_priority = rules['data_priority']

    def process_string(str_):
        """
        Standardises a string, and splits it in case multiple subjects are in
        it. Returns a list
        """
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

    def process_dict(dict_):
        """
        Process dict data, to extract standardises strings used to convert to
        subject data
        """
        standard_strings = []
        for key in dict_key_priority:
            if key in dict_:
                dat = dict_[key]
                if isinstance(dat, str):
                    standard_strings.extend(process_string(dat))
                    break
                elif isinstance(dat, list):
                    standard_strings.extend(process_list(dat))
                    break

        return standard_strings

    def process_list(list_):
        """
        Process list data, to extract standardised strings
        """
        if len(list_) > max_sarray_length:
            return []

        standard_strings = []
        for item in list_:
            if isinstance(item, str):
                standard_strings.extend(process_string(item))
            elif isinstance(item, dict):
                standard_strings.extend(process_dict(item))

        return standard_strings

    def convert_string(str_):
        """
        Converts standardised string data into a subject
        """
        if str_ in translated_subjects:
            return subject_mapping[str_]
        else:
            return []

    def remove_parents_relations(subject_list):
        """
        Remove parents and relations from a list of subjects, keeps only
        lowest level unique subjects. Also removes relations of relations
        """
        relations_parents = set()
        for subject in subject_list:
            relations_parents.update(subject_scheme_data[subject]
                                     ['all_parents_relations'])

        return [s for s in subject_list if s not in relations_parents]

    # Search trough each key to find subject data
    found_subjects = []
    for key in data_priority:
        if key in candidates:
            value = candidates[key]
            if isinstance(value, str):
                standardised_names = process_string(value)
            elif isinstance(value, list):
                standardised_names = process_list(value)
            elif isinstance(value, dict):
                standardised_names = process_dict(value)
            elif value is None:
                continue
            for name in standardised_names:
                subjects = convert_string(name)
                # Make sure only unique are added and only lowest level
                # childs are used in count
                found_subjects = list(set(found_subjects + subjects))
                found_subjects = remove_parents_relations(found_subjects)

            if len(found_subjects) > max_subjects:
                found_subjects = []  # Data is most likely crap
                break

    if found_subjects == [] or len(found_subjects) > max_subjects:
        return None

    # Add all parents again, for proper ES aggregations, and simplified
    # search from front-end
    total_subjects = set()
    for subject in found_subjects:
        total_subjects.add(subject)
        total_subjects.update(
            subject_scheme_data[subject]['all_parents_relations']
        )
    total_subjects = list(total_subjects)

    # Since low_level_count is needed for scoring, it's exported. Final format
    # is done in post_processing.score, where the 'low_level' is used
    return {'all': total_subjects, 'low_level': found_subjects}


def location(candidates):
    """
    Translate location information about a resource into the new metadata
    format

    Parameters:
        candidates --- dict: Key value combinations that possibly contain
        spatial data about a resource

    Returns:
        Translated Entry --- array/NoneType: 1 or more locations, or None if no
        information is detected in the candidates
    """
    rules = trl_rules['location']
    # TODO: name_min_len and max_len are currently not enforced
    # min_name_len = rules['children']['name']['length']['min']
    # max_name_len = rules['children']['name']['length']['max']
    data_priority = rules['data_priority']
    bbox_pairs = rules['bboxPairs']
    dict_bbox_pairs = rules['dictBBOXPairs']
    results = []

    class BBOXGeometry(dict):
        """
        A BBOX (Envelope) GeoJSON feature (ElasticSearch spec), or point if
        given coordinates are the same
        """

        def __init__(self, xmin, ymin, xmax, ymax):
            r_xmin = round(xmin, 2)
            r_ymin = round(ymin, 2)
            r_xmax = round(xmax, 2)
            r_ymax = round(ymax, 2)
            if r_xmin == r_xmax and r_ymin == r_ymax:
                self.update({
                    'type': 'Point',
                    'coordinates': [xmin, ymin]
                })
            else:
                self.update({
                    'type': 'envelope',
                    'coordinates': [[xmin, ymax], [xmax, ymin]]
                })

    class Location(dict):
        """
        A Location description in the new metadata scheme
        """

        def __init__(self, name=None, geometry=None, elevation=None):
            """
            Create a location dict in the new metadata schema
            """
            all_none = True

            if name is not None:
                all_none = False
                self['name'] = str(name)
            if geometry is not None:
                all_none = False
                self['geometry'] = dict(geometry)
            if elevation is not None:
                all_none = False
                self['elevation'] = float(elevation)

            if all_none:
                raise ValueError('At least one argument should have a value!')

    def bbox_valid(xmin, ymin, xmax, ymax):
        """
        Checks if given bbox data is valid
        """
        return (xmin < xmax and ymin < ymax and xmin >= -180 and xmax <= 180
                and ymin >= -90 and ymax <= 90
                and not (xmin == xmax == ymin == ymax == 0)
                and not (xmin == -180 and xmax == 180 and ymin == -90 and ymax == 90))

    def point_valid(x, y):
        """
        Checks if given point location is valid
        """
        return ((x >= -180 and x <= 180) and (y >= -90 and y <= 90)
                and not (x == 0 and y == 0))

    def results_contain_geometry(results):
        """
        Check for geometry data in results
        """
        if results == []:
            return False

        for result in results:
            if 'geometry' in result:
                return True
        else:
            return False

    def process_geometry(item):
        """
        Process single geometry to return relevant information
        """
        if item.type == 'Point':
            if not point_valid(item.x, item.y):
                return None
            return dict(Location(geometry=geometry.mapping(item)))
        else:
            xmin, ymin, xmax, ymax = item.bounds
            if not bbox_valid(xmin, ymin, xmax, ymax):
                return None
            return dict(Location(geometry=BBOXGeometry(*item.bounds)))

    def handle_shape(shape):
        """
        Handle shapely shape data
        """
        results = []

        try:
            if 'multi' in shape.type.lower():
                items = list(shape)
                for item in items:
                    result = process_geometry(item)
                    if result is not None:
                        results.append(result)
            else:
                result = process_geometry(shape)
                if result is not None:
                    results.append(result)
        except TypeError:
            return []

        return results

    def handle_geojson(dict_):
        """
        Convert GeoJSON data into the new metadata format
        """
        try:
            shape = geometry.asShape(dict_)
            if not shape.is_empty:
                return handle_shape(shape)
            else:
                return []
        except ValueError:
            return []

    def handle_wkt(str_):
        """
        Convert WKT geodata into the new metadata format
        """
        try:
            shape = wkt.loads(str_)
            return handle_shape(shape)
        except WKTReadingError:
            return []

    def handle_string(str_):
        """
        Derive results from string data
        """
        results = []

        # Test for GeoJSON, WKT or extent data, else handle as name:
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
                results = handle_geojson(geojson_data)
        elif str_.startswith('ENVELOPE('):
            # SOLR geom ENVELOPE format
            coordinate_string = str_.strip('ENVELOPE() ')
            try:
                coords = [float(c) for c in coordinate_string.split(',')]
                xmin, xmax, ymax, ymin = coords
            except (ValueError):
                return []

            if not bbox_valid(xmin, ymin, xmax, ymax):
                return []

            loc = dict(Location(geometry=BBOXGeometry(
                xmin,
                ymin,
                xmax,
                ymax
            )))
            results = [loc]
        elif wkt_format_regex.match(str_):
            results = handle_wkt(str_)
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

            if not bbox_valid(xmin, ymin, xmax, ymax):
                return []

            loc = dict(Location(geometry=BBOXGeometry(
                xmin,
                ymin,
                xmax,
                ymax
            )))
            results = [loc]
        else:
            bbox_match = bbox_key_value_pattern.match(str_.lower().strip())
            if bbox_match is not None:
                bbox_dict = {}
                for key_i, value_i in bbox_kv_groups:
                    key = bbox_match.group(key_i)
                    value = bbox_match.group(value_i)
                    bbox_dict[key] = value
                results = handle_dict(bbox_dict)

        return results

    def handle_dict(dict_):
        """
        Convert dict data into the new metadata format
        """
        results = []
        if 'coordinates' in dict_:
            if 'type' in dict_ and dict_['type'] != 'envelope':
                results = handle_geojson(dict_)
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
                        if not bbox_valid(xmin, ymin, xmax, ymax):
                            if xmin == xmax and ymin == ymax:
                                if point_valid(xmin, ymin):
                                    results.append(
                                        dict(Location(geometry=BBOXGeometry(
                                            xmin,
                                            ymin,
                                            xmax,
                                            ymax
                                            ))))
                            else:
                                return []
                        results.append(dict(Location(geometry=BBOXGeometry(
                            xmin,
                            ymin,
                            xmax,
                            ymax
                        ))))
        elif 'LowerCorner' in dict_ and 'UpperCorner' in dict_:
            # Format used by CSW/Geonetwork
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

                        if bbox_valid(xmin, ymin, xmax, ymax):
                            results.append(dict(Location(geometry=BBOXGeometry(
                                xmin,
                                ymin,
                                xmax,
                                ymax
                            ))))
                        elif xmin == xmax and ymin == ymax:
                            if point_valid(xmin, ymin):
                                results.append(
                                    dict(Location(geometry=BBOXGeometry(
                                        xmin,
                                        ymin,
                                        xmax,
                                        ymax
                                        ))))
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
                        if bbox_valid(xmin, ymin, xmax, ymax):
                            results.append(dict(Location(geometry=BBOXGeometry(
                                xmin,
                                ymin,
                                xmax,
                                ymax
                            ))))
                    except ValueError:
                        pass
        else:
            # Check if any of the dictBBOXPairs are in the dict:
            for pair in dict_bbox_pairs:
                for bbox_key in pair:
                    if bbox_key not in dict_:
                        break
                else:
                    xminkey, yminkey, xmaxkey, ymaxkey = pair

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

                    if bbox_valid(xmin, ymin, xmax, ymax):
                        results.append(dict(Location(geometry=BBOXGeometry(
                            xmin,
                            ymin,
                            xmax,
                            ymax
                        ))))
                    elif xmin == xmax and ymin == ymax:
                        if point_valid(xmin, ymin):
                            results.append(
                                dict(Location(geometry=BBOXGeometry(
                                    xmin,
                                    ymin,
                                    xmax,
                                    ymax
                                    ))))
                    break
            else:
                # No valid bbox pairs are found, try final options
                if st.REP_TEXTKEY in dict_:
                    value = dict_[st.REP_TEXTKEY]
                    if isinstance(value, str):
                        return handle_string(value)

                # Format in ANDS:
                if dict_.get('type') == 'coverage' and 'spatial' in dict_:
                    if isinstance(dict_['spatial'], dict):
                        return handle_dict(dict_['spatial'])

                # Get other keys
                fetch_key = None
                for key in ['geographicElement', 'geom']:
                    if key in dict_:
                        fetch_key = key
                        break

                if fetch_key:
                    ggel = dict_[fetch_key]
                    if isinstance(ggel, dict):
                        return handle_dict(ggel)
                    elif isinstance(ggel, list):
                        return handle_list(ggel)
                    elif isinstance(ggel, str):
                        return handle_string(ggel)

        return results

    def handle_list(list_):
        """
        Handle list data
        """
        results = []
        for item in list_:
            if isinstance(item, str):
                reslist = handle_string(item)
                results += reslist
            elif isinstance(item, dict):
                reslist = handle_dict(item)
                results += reslist
            elif isinstance(item, list):
                reslist = handle_list(item)
                results += reslist

        return results

    def filter_duplicates(locations):
        """
        Filter (near) duplicates from a list of locations
        """
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
        l_names = set()
        for ind_, loc in enumerate(locations):
            geom = loc.get('geometry')
            name = loc.get('name')
            if geom is not None:
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

            if name is not None:
                if name.lower() not in l_names:
                    l_names.add(name.lower())
                else:
                    if geom is None:
                        duplicate_inds.add(ind_)
                    else:
                        # Geometry is apperently unique, see if the name
                        # Occurs in an entry without geometry:
                        match = [[l, ind_] for ind_, l
                                 in enumerate(locations[:ind_])
                                 if 'name' in l and l['name'].lower() == name
                                 and ind_ not in duplicate_inds]

                        if 'geometry' not in match[0][0]:
                            duplicate_inds.add(match[0][1])

        out_locs = [l for ind_, l in enumerate(locations) if ind_ not in
                    duplicate_inds]

        return out_locs

    # First check for BBOX keys, since these need to be combined:
    first_keys = [pair[0] for pair in bbox_pairs]
    for ind_, test_key in enumerate(first_keys):
        if test_key in candidates:
            try:
                bbox_coords = [float(candidates[k]) for k in bbox_pairs[ind_]]
                xmin, ymin, xmax, ymax = bbox_coords
                if bbox_valid(*bbox_coords):
                    feature = BBOXGeometry(*bbox_coords)
                    results.append(dict(Location(geometry=feature)))
                    break
                elif xmin == xmax and ymin == ymax:
                    if point_valid(xmin, ymin):
                        results.append(
                            dict(Location(geometry=BBOXGeometry(
                                xmin,
                                ymin,
                                xmax,
                                ymax
                                ))))
            except (TypeError, ValueError, KeyError):
                # If a wrong format is in one or more of the fields, ignore it
                pass

    # Otherwise, examine the rest of the keys, in order specified
    for key in data_priority:
        if key in candidates:
            payload = candidates[key]
            if isinstance(payload, str):
                new_results = handle_string(payload)
            elif isinstance(payload, dict):
                new_results = handle_dict(payload)
            elif isinstance(payload, list):
                new_results = handle_list(payload)

            results = results + new_results

    results = filter_duplicates(results)

    # If there is one entry with only a name, and one with only geometry,
    # merge these entries
    if len(results) == 2:
        geom = None
        name = None
        for ind_, loc in enumerate(results):
            if len(loc) == 1:
                if 'geometry' in loc:
                    geom = loc['geometry']
                elif 'name' in loc:
                    name = loc['name']
        if geom is not None and name is not None:
            results = [{'geometry': geom, 'name': name}]

    if results == []:
        results = None

    return results


def spatial_resolution(candidates):
    """
    Translate information about the spatial resolution of a resource
    """
    raise NotImplementedError


def time_period(candidates):
    """
    Translate information about the time periods related to a resource
    """
    rules = trl_rules['timePeriod']
    lt_date = rules['children']['start']['lt']
    gt_date = rules['children']['start']['gt']
    premove = rules['period_remove']
    pstart_keys = rules['start_dict_keys']
    pend_keys = rules['end_dict_keys']
    seperators = rules['period_seperators']
    default_type = 'About'

    class TimePeriod(dict):
        """
        Time period object for the new metadata scheme
        """

        def __init__(self, startdate, enddate, type_):
            self.validate_params(startdate, enddate, type_)
            self.update({
                'type': type_,
                'start': _date2str(startdate),
                'end': _date2str(enddate)
            })
            self.starts = startdate
            self.ends = enddate

        def validate_params(self, startdate, enddate, type_):
            if type_ not in ['Valid', 'Collected', 'About']:
                raise ValueError('Invalid type specified!')
            elif not isinstance(startdate, datetime.datetime):
                raise TypeError('Start date not a datetime object')
            elif not isinstance(enddate, datetime.datetime):
                raise TypeError('Start date not a datetime object')
            elif startdate > enddate:
                raise ValueError('Startdate later than enddate')

    def handle_string(str_):
        """
        Translates string data into a date range. Returns a list with a
        TimePeriod instance. If data could not be parsed, an empty list is
        returned
        """
        start_date = None
        end_date = None
        s = str_.lower()
        for rm in premove:
            s = s.replace(rm, '')
        if len(s) > 64:
            return []
        if s.lower().startswith('r/'):
            start_payload = s.split('/')[1]
            end_payload = 'now'
            start_date = _str2date(start_payload, lt_date, gt_date,
                                   ignore_now=True)
            end_date = _str2date(end_payload, lt_date, gt_date,
                                 period_end=True)
        else:
            for sep in seperators:
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

                        start_date = _str2date(start_payload, lt_date, gt_date,
                                               ignore_now=True)
                        end_date = _str2date(end_payload, lt_date, gt_date,
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
                    end_date = _str2date('now', lt_date, gt_date,
                                         period_end=True)
                elif endswith_duration or (years and len(years) == 1):
                    if endswith_duration:
                        start_payload = parts[0]
                        end_payload = _parse_ISO_duration(parts[-1])
                    else:
                        start_payload = s.strip('/-')
                        # Assume a single day/month/year coverage
                        end_payload = start_payload

                    start_date = _str2date(start_payload, lt_date, gt_date,
                                           ignore_now=True)

                    if start_date is None:
                        return []

                    end_date = None
                    if isinstance(end_payload, str):
                        end_date = _str2date(end_payload, lt_date, gt_date,
                                             period_end=True)
                    elif isinstance(end_payload, datetime.timedelta):
                        # In case a duration is parsed
                        end_date = start_date + end_payload

                    if end_date is None:
                        # Assume now, if no other can be found
                        end_date = _str2date(
                            'now', lt_date, gt_date, period_end=True
                        )

                else:
                    return []

        if start_date > end_date:
            return []

        tperiod = TimePeriod(start_date, end_date, default_type)

        return [tperiod]

    def handle_dict(dict_):
        """
        Translates dict data into a date range. Returns a list with a single
        TimePeriod instance, or an empty list if no suitable data is found
        """
        # A valid start date must be available:
        start_date = None
        for sdate_key in pstart_keys:
            if sdate_key in dict_:
                sdate_value = dict_[sdate_key]
                if isinstance(sdate_value, str):
                    start_date = _str2date(dict_[sdate_key], lt_date, gt_date,
                                           ignore_now=True)
                    if start_date is not None:
                        break
                elif isinstance(sdate_value, int):
                    start_date = _parse_timestamp(sdate_value, lt_date,
                                                  gt_date)
                    if start_date is not None:
                        break
        else:
            # If no start date was found, return None
            return []

        end_date = None
        for edate_key in pend_keys:
            if edate_key in dict_:
                edate_value = dict_[edate_key]
                if isinstance(edate_value, str):
                    end_date = _str2date(edate_value, lt_date, gt_date,
                                         True)
                    if end_date is not None:
                        break
                elif isinstance(edate_value, int):
                    end_date = _parse_timestamp(edate_value, lt_date, gt_date)
                    if end_date is not None:
                        break
        else:
            # If no end date was found, we assume it is now:
            end_date = _str2date('now', lt_date, gt_date, True)

        if start_date > end_date:
            return []

        tperiod = TimePeriod(start_date, end_date, default_type)

        return [tperiod]

    def handle_list(list_):
        """
        Translates list data into a time range. Returns a list of timePeriod
        instances. In case no data is found, an empty list is returned
        """
        data = []
        for item in list_:
            if isinstance(item, str):
                data += handle_string(item)
            elif isinstance(item, dict):
                data += handle_dict(item)

        return data

    def handle_payload(payload):
        """
        Translates a payload into a date range. Returns a list of TimePeriod
        instances. In case no data is found, an empty list is returned
        """
        data = []
        if isinstance(payload, str):
            data += handle_string(payload)
        elif isinstance(payload, list):
            data += handle_list(payload)
        elif isinstance(payload, dict):
            data += handle_dict(payload)

        return data

    def merge_overlapping(time_periods):
        """
        Post process a list of time periods, to find overlapping timeperiods
        and merge these
        """
        filtered_periods = []
        for t_period in time_periods:
            # Check if there is a complete or partial overlap with a previously
            # added period:
            merge_with = []
            delete_old = []
            for ind_, f_period in enumerate(filtered_periods):
                if t_period.starts > f_period.starts:
                    if t_period.starts > f_period.ends:
                        # Can be added, so continue
                        continue
                    elif t_period.starts == f_period.ends:
                        # Periods form continous period
                        # added to list, since long event can link multiple
                        # together
                        merge_with.append(ind_)
                    else:
                        # t_period.starts < f_period.ends
                        if t_period.ends > f_period.ends:
                            # Partial overlap, merge both
                            merge_with.append(ind_)
                        elif t_period.ends == f_period.ends:
                            # Old completely overlaps new, keep old
                            break
                        else:
                            # t_period.ends < f_period.ends
                            # Old completely overlaps new, keep old
                            break
                elif t_period.starts == f_period.starts:
                    if t_period.ends > f_period.ends:
                        # New completely overlaps old, delete old, keep new:
                        delete_old.append(ind_)
                        # Continue to check if it overlaps with something else:
                        continue
                    elif t_period.ends == f_period.ends:
                        # New exactly matches old, disregard new
                        break
                    else:
                        # t_period.ends < f_period.ends
                        # Old completely overlaps with new, disregard new
                        break
                else:
                    # t_period.starts < f_period.starts
                    if t_period.ends > f_period.starts:
                        if t_period.ends > f_period.ends:
                            # New completely overlaps old, delete old
                            delete_old.append(ind_)
                        elif t_period.ends == f_period.ends:
                            # New completely overlaps old, delete old
                            delete_old.append(ind_)
                        else:
                            # t_period.ends < f_period.ends:
                            # Partial overlap, merge both
                            merge_with.append(ind_)
                    elif t_period.ends == f_period.starts:
                        # Both form a continuous period, merge them:
                        merge_with.append(ind_)
                    else:
                        # t_period.ends < f_period.starts
                        # No overlap, new can be added, so continue
                        continue
            else:
                if merge_with != []:
                    # If there are also entries in 'delete_old', these have to
                    # be handled simultaneously, because otherwise indices are
                    # no longer valid
                    merge_delete_dict = {'merge': i for i in merge_with}
                    merge_delete_dict.update(
                        {'delete': i for i in delete_old}
                    )
                    merge_delete_dict = dict(sorted(merge_delete_dict.items(),
                                                    key=lambda x: x[1],
                                                    reverse=True))
                    start_dates = []
                    end_dates = []
                    for action, ind_ in merge_delete_dict.items():
                        if action == 'merge':
                            start_dates.append(filtered_periods[ind_].starts)
                            end_dates.append(filtered_periods[ind_].ends)
                        del filtered_periods[ind_]

                    start_dates.append(t_period.starts)
                    end_dates.append(t_period.ends)

                    merged_sdate = min(start_dates)
                    merged_edate = max(end_dates)

                    filtered_periods.append(TimePeriod(merged_sdate,
                                                       merged_edate,
                                                       default_type))
                elif delete_old != []:
                    # Only delete_old, so no merge. This means t_period still
                    # has to be added after deletion of old one
                    delete_old = sorted(delete_old, reverse=True)
                    for ind_ in delete_old:
                        del filtered_periods[ind_]
                    filtered_periods.append(t_period)
                else:
                    # No conflicts were found, so t_period can be added:
                    filtered_periods.append(t_period)

        return filtered_periods

    time_period_data = []

    # When there are two seperate keys giving a beginning and end, extract
    # these data
    for sdate_key, edate_key in rules['begin_endkeys']:
        if sdate_key in candidates and isinstance(candidates[sdate_key], str):
            start_date = _str2date(candidates[sdate_key], lt_date, gt_date,
                                   ignore_now=True)
            if start_date is None:
                continue

            end_date = None
            if edate_key in candidates and\
                    isinstance(candidates[edate_key], str):
                end_date = _str2date(candidates[edate_key], lt_date, gt_date,
                                     True)
            if end_date is None:
                end_date = _now_date()

            if start_date > end_date:
                continue

            time_period_data.append(TimePeriod(start_date, end_date,
                                               default_type))

    # Try to find complete period data in the other keys mapped to timePeriod
    for key in rules['data_priority']:
        if key in candidates:
            time_period_data += handle_payload(candidates[key])

    # Since the same period could have been under multiple keys, a check for
    # Overlapping periods is performed:
    time_period_data = merge_overlapping(time_period_data)

    # Convert timePeriod objects to dicts:
    time_period_data = [dict(p) for p in time_period_data]

    if time_period_data == []:
        time_period_data = None

    return time_period_data


def temporal_resolution(candidates):
    """
    Translate data about 'temporalResolution' into the new metadata schema
    """
    raise NotImplementedError


def format(candidates):
    """
    Translation data about 'format' into the new metadata schema
    """
    rules = trl_rules['format']

    def derive_plain_extensions(str_):
        """
        Derive one or more file extensions directly from a string
        """
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

    def handle_string(str_):
        """
        Convert format string data into the new metadata schema. Returns a list
        of formats. Empty if nothing valid is found
        """
        data = []

        str_ = str_.lower().replace('zipped ', '').replace(' file', '')

        if str_ in file_format_mapping:
            data.append(file_format_mapping[str_])
        else:
            if '(' in str_:
                matches = between_brackets_regex.findall(str_)
                for match in matches:
                    data += derive_plain_extensions(match)
            else:
                data += derive_plain_extensions(str_)

        return data

    def handle_dict(dict_):
        """
        Convert dict data into the new metadata format. Returns a list of
        formats, empty if nothing is found
        """
        raise NotImplementedError

    def handle_list(list_):
        """
        Convert format list data into the new metadata schema. Returns a list
        of formats. Empty if nothing is found
        """
        data = []
        for item in list_:
            if isinstance(item, str):
                data += handle_string(item)

        return data

    def handle_payload(payload):
        """
        Handle any type of format data, and transform it to the new metadata
        format
        """
        data = []

        if isinstance(payload, str):
            data += handle_string(payload)
        elif isinstance(payload, list):
            data += handle_list(payload)
        # elif isinstance(payload, dict):
        #     data += handle_dict(payload)

        return data

    formats = []
    for key in rules['data_priority']:
        if key in candidates:
            formats += handle_payload(candidates[key])

    if formats == []:
        formats = None
    else:
        formats = list(set(formats))

    return formats


def size(candidates):
    """
    Translate data about 'size' into the new metadata schema
    """
    raise NotImplementedError


def delimiter(candidates):
    """
    Translate data about 'delimiter' into the new metadata schema
    """
    raise NotImplementedError


def fingerprint(candidates):
    """
    Translate data about 'fingerprint' into the new metadata schema
    """
    raise NotImplementedError


def language(candidates):
    """
    Translate data about 'language' into the new metadata schema
    """
    rules = trl_rules['language']

    def handle_string(str_):
        """
        Converts string data into one or multiple languages

        Arguments:
            str_ --- str: The string to be converted

        Returns:
            list --- The detected languages, empty if nothing is found (can
            still contain duplicates)
        """
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

        return langs

    def handle_list(list_):
        """
        Converts list data into one or multiple languages

        Arguments:
            list_ --- list: The list to be converted

        Returns:
            list --- The detected languages, empty if nothing is found (can
            still contain duplicates)
        """
        langs = []
        for item in list_:
            if isinstance(item, str):
                langs += handle_string(item)
            elif isinstance(item, dict):
                langs += handle_dict(item)

        return langs

    def handle_dict(dict_):
        """
        Converts dict data into one or multiple languages

        Arguments:
            dict_ --- dict: The dict to be converted

        Returns:
            list --- The detected languages, empty if nothing is found (can
            still contain duplicates)
        """
        langs = []
        for key in rules['dict_key_priority']:
            if key in dict_:
                value = dict_[key]
                if isinstance(value, str):
                    langs += handle_string(value)
                elif isinstance(value, list):
                    langs += handle_list(value)

        return langs

    languages = []
    for key in rules['data_priority']:
        if key in candidates:
            value = candidates[key]
            if isinstance(value, str):
                languages += handle_string(value)
            elif isinstance(value, list):
                languages += handle_list(value)
            elif isinstance(value, dict):
                languages += handle_dict(value)
    if languages == []:
        languages = None
    else:
        languages = list(set(languages))

    return languages


def sample_size(candidates):
    """
    Translate data about 'sampleSize' into the new metadata schema
    """
    raise NotImplementedError


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
