# -*- coding: utf-8 -*-
"""
POST PROCESSING MODULE

This module includes functions related to translating the metadata from
external data sources to the correct metadata format.
"""
from statistics import mean
import re

from metadata_ingestion import _loadcfg

from metadata_ingestion.translate import subject_scheme_data

depth_score = {
    0: 0.6,
    1: 0.8,
    2: 0.95,
    3: 1
}

amount_score = {
    1: 0.4,
    2: 0.8,
    3: 1,
    4: 0.7,
    5: 0.5
}

desc_sc = [[0, 0.20], [400, 1], [1000, 1], [2048, 0.5]]
title_sc = [[0, 0], [50, 1], [100, 1], [256, 0.5]]
thesis_papers_books_types = set([
    "Document:Thesis",
    "Document:Paper",
    "Document:Book"
])
figure_regex = re.compile(r'fig(ure)?\.?\s\d')
text_formats = set(['PDF', 'TXT', 'DOC', 'DOCX', 'RTF'])

postfilters = _loadcfg.postfilters()


def get_subject_depth(subject_id):
    """
    Determine the depth of a subject in the hierarchy (least deep mention)
    """
    s_data = subject_scheme_data[subject_id]
    all_parents = s_data['parents'] + s_data['relations']
    if len(all_parents) == 0:
        return 0
    else:
        depths = []
        for parent in all_parents:
            depths.append(get_subject_depth(parent))
        min_depth = min(depths)
        return min_depth + 1


def interp_curve(x, curve):
    """Interpolate between points in curve (linear interpolation)"""
    if x <= curve[0][0]:
        return curve[0][1]
    elif x >= curve[-1][0]:
        return curve[-1][1]

    for i, val in enumerate(curve):
        if x <= val[0]:
            break
    min_x = curve[i-1][0]
    max_x = val[0]
    min_y = curve[i-1][1]
    max_y = val[1]
    return (((x - min_x) / (max_x - min_x)) * (max_y - min_y)) + min_y

# \\\\\\\\\\\\\\\\\\\ DEFINE FILTER FUNCTIONS BELOW //////////////////////////


def _FILTER_notitle(entry):
    """
    Returns True if the entry does not have a title
    """
    return 'title' not in entry


def _FILTER_sparse_metadata(entry):
    """
    Returns True if the entry has no abstractORDescription and also no subject
    and location
    """
    return ('abstractORdescription' not in entry and 'subject' not in entry
            and 'location' not in entry)


def _FILTER_invalid_type(entry):
    """
    Filters entries with invalid type. Entries with unknown type will pass
    """
    entry_types = entry.get('type')

    if entry_types is not None:
        return 'INVALID' in entry_types
    else:
        return False


def _FILTER_invalid_title_description(entry):
    """
    Entries with specific phrases in the title or description are filtered
    """
    title = entry.get('title')
    if title is not None:
        title_std = title.lower()
        if (title_std.startswith('appendix')
                or 'supplementary material' in title_std
                or 'supplemental material' in title_std
                or figure_regex.search(title_std)):
            return True

    description = entry.get('abstractORdescription')
    if description is not None:
        desc_std = description.lower()
        if (desc_std.startswith('photograph of')
                or desc_std.startswith('photographs of')):
            return True

    return False


def _FILTER_plusone_dois(entry):
    """
    Filter entries with a DOI from the PlusOne journal. This is mostly junk
    """
    identifier = entry.get('identifier')
    if identifier is not None:
        if (identifier['type'] == 'DOI'
                and 'journal.pone' in identifier['value']):
            return True

    return False


def _FILTER_theses_papers_books(entry):
    """
    Filters all theses, papers and books, since there are other (also more
    complete) websites to search for these (e.g. google scholar...)
    """
    entry_types = entry.get('type')

    if entry_types is not None:
        for type_ in entry_types:
            if type_ in thesis_papers_books_types:
                return True

    # If there are no matches, return False
    return False


def _FILTER_key_value_pairs(entry):
    """
    Filters all theses, papers and books, since there are other (also more
    complete) websites to search for these (e.g. google scholar...)
    """
    for key, options in postfilters.items():
        if key in entry:
            value = entry[key]
            if isinstance(options, dict):
                for k, filterset in options.items():
                    subvalue = value[k]
                    if subvalue in filterset:
                        return True
            elif value in options:
                return True

    return False

# \\\\\\\\\\\\\\\\\\\ DEFINE OPTIMIZE FUNCTIONS BELOW /////////////////////////


def _OPTIMIZE_dates(entry):
    """
    Optimizes the modified, created and issued dates
    """
    org_created = entry.get('created')
    if org_created is not None:
        org_issued = entry.get('issued')
        org_modified = entry.get('modified')
        if org_issued is not None:
            # The issued date has to be great then or equal to
            # the created date, otherwise it's invalid
            if org_issued < org_created:
                # Take the lowest. Except if the issued date is the first of
                # January, indicating it was parsed from a year, the created
                # Date is most likely more detailed
                if not org_issued.endswith('01-01'):
                    entry['created'] = org_issued
                    del entry['issued']
                else:
                    del entry['issued']

        if org_modified is not None:
            created = entry['created']  # May be changed by above...
            if org_modified < created:
                entry['created'] = org_modified
                del entry['modified']
                # Restore the 'issued', when created has become lower...
                if org_issued is not None and 'issued' not in entry\
                        and entry['created'] < org_issued:
                    entry['issued'] = org_issued


def _OPTIMIZE_creator_publisher(entry):
    """
    If the creator is also the publisher, the creator is removed. If there are
    multiple creators, only the one that matches the publisher is removed
    """
    if 'creator' in entry and 'publisher' in entry:
        p_name = entry['publisher']['name'].strip().lower()

        remove_inds = []
        for ind_, c in enumerate(entry['creator']):
            if 'name' in c:
                name = c['name']
            else:
                name = c['organization']

            if name.strip().lower() == p_name:
                remove_inds.append(ind_)

        if remove_inds != []:
            if len(remove_inds) == len(entry['creator']):
                del entry['creator']
            else:
                for i in reversed(remove_inds):
                    del entry['creator'][i]


def _OPTIMIZE_document_types(entry):
    """
    If an entry only has text based formats, and the title contains specific
    phrases, it can be classified as a Document or Document:Report
    """
    formats = entry.get('format')
    title = entry.get('title')
    if formats is None or title is None:
        return

    has_nontext_formats = bool(set(formats).difference(text_formats))
    if has_nontext_formats:
        return

    for phrase in ['paper', 'guideline', 'guidelines', 'poster']:
        if phrase in title:
            entry['type'] = ['Document']
            return

    if 'report' in title:
        entry['type'] = ['Document', 'Document:Report']
        return


# \\\\\\\\\\\\\\\\\\\ DEFINE SCORING FUNCTIONS BELOW //////////////////////////


def _SCORE_metadata_quality(entry):
    """
    Add a metadata score that can be used in sorting results. Please not that
    this should be done before adding global platform attributes, since these
    do not say anything about score
    """
    # Each relevant metadata attribute has it's own function
    def description_score():
        description = entry.get('abstractORdescription')
        if description is not None:
            # Using a skewed distribution to score length
            return interp_curve(len(description), desc_sc)
        else:
            return 0

    def title_score():
        title = entry['title']
        return interp_curve(len(title), title_sc)

    def subject_score():
        subject = entry.get('subject')
        if subject is not None:
            low_level_subjects = subject['low_level']
            # Determine individual subject scores
            s_scores = []
            for subject_id in low_level_subjects:
                depth = get_subject_depth(subject_id)
                s_scores.append(depth_score[depth])
            avg_score = mean(s_scores)
            # Now score for amount
            amount = len(s_scores)

            # Since the 'low_level' is no longer used, replace subject data
            entry['subject'] = subject['all']

            return amount_score[amount] * avg_score
        else:
            return 0

    def location_score():
        """If one or multiple locations available, score is one"""
        if len(entry.get('location', [])) > 0:
            return 1
        else:
            return 0

    def dates_score():
        """One date, 0.5, two dates 0.75 and three 1.0"""
        count = 0
        for date_key in ['modified', 'created', 'issued']:
            if entry.get(date_key) is not None:
                count += 1

        if count > 0:
            return 0.5 + ((count - 1)/2) * 0.5
        else:
            return 0

    def timeperiod_score():
        """Only based on existence"""
        if entry.get('timePeriod') is not None:
            return 1
        else:
            return 0

    def license_score():
        """Scores license name only, otherwise it's half-put-together"""
        if entry.get('license', {}).get('name') is not None:
            return 1
        else:
            return 0

    # Calculate all seperate scores
    scores = {
        'description': description_score(),
        'title': title_score(),
        'subject': subject_score(),
        'location': location_score(),
        'dates': dates_score(),
        'timePeriod': timeperiod_score(),
        'license': license_score()
    }

    # Calculate composite score
    scores['total'] =\
        0.25 * scores['description'] +\
        0.25 * scores['title'] +\
        0.15 * scores['subject'] +\
        0.15 * scores['location'] +\
        0.10 * scores['dates'] +\
        0.05 * scores['timePeriod'] +\
        0.05 * scores['license']

    entry['_metadata_scores'] = scores


# DEFINE ALL FILTERS AND OPTIMIZATIONS ABOVE!!!!
filter_funcs = [fname for fname in dir() if fname.startswith('_FILTER_')]
optimize_funcs = [fname for fname in dir() if fname.startswith('_OPTIMIZE_')]
score_funcs = [fname for fname in dir() if fname.startswith('_SCORE_')]


def is_filtered(entry):
    """
    Returns True if an entry should be filtered, False if it should not be
    filtered
    """
    for fname in filter_funcs:
        if globals()[fname](entry):
            return True
    else:
        return False


def optimize(entry):
    """
    Runs the post-optimizations over the entry
    """
    for fname in optimize_funcs:
        globals()[fname](entry)


def score(entry):
    """
    Run the score functions
    """
    for fname in score_funcs:
        globals()[fname](entry)
