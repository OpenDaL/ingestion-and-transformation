# -*- coding: utf-8 -*-
"""
Module with Post-Processors

Each Post-Processor handles a different part of the post processing. From the
client code, use the MetadataPostProcessor class, that combines all
PostProcessors

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
from statistics import mean
import re
from abc import ABC, abstractmethod

from metadata_ingestion import _loadcfg

from metadata_ingestion.resource import ResourceMetadata


class PostProcessor(ABC):
    """
    Base class for all PostProcessors
    """

    @abstractmethod
    def post_process(self, metadata: ResourceMetadata):
        """
        Post-process the provided metadata by updating the input object
        """
        pass


class Filter(PostProcessor):
    """
    Filters PostProcessor

    Filter all metadata that:
        * Does not have a title
        * Has no description, subject or location
        * Contains an invalid type (INVALID or thesis, book , paper)
        * Has an invalid title or description
        * Have 'plusone' DOIs
        * Contains configured key/value combinations
    """
    thesis_papers_books_types = set([
        "Document:Thesis",
        "Document:Paper",
        "Document:Book"
    ])
    figure_regex = re.compile(r'fig(ure)?\.?\s\d')
    postfilters = _loadcfg.postfilters()

    def _has_no_title(self, translated_metadata: dict) -> bool:
        return 'title' not in translated_metadata

    def _has_sparse_metadata(self, translated_metadata: dict) -> bool:
        return (
            'description' not in translated_metadata and
            'subject' not in translated_metadata and
            'location' not in translated_metadata
        )

    def _contains_unwanted_type(self, translated_metadata: dict) -> bool:
        if 'type' not in translated_metadata:
            return False

        types = translated_metadata['type']
        return (
            'INVALID' in types or
            set(types).intersection(self.thesis_papers_books_types)
        )

    def _contains_invalid_title_or_description(
            self, translated_metadata: dict,
            ) -> bool:
        if 'title' in translated_metadata:
            title = translated_metadata['title']
            lower_title = title.lower()
            if (
                    lower_title.startswith('appendix')
                    or 'supplementary material' in lower_title
                    or 'supplemental material' in lower_title
                    or self.figure_regex.search(lower_title)
                    ):
                return True

        if 'description' in translated_metadata:
            description = translated_metadata['description']
            lower_description = description.lower()
            if (
                    lower_description.startswith('photograph of') or
                    lower_description.startswith('photographs of')
                    ):
                return True

        return False

    def _has_plusone_identifier(self, translated_metadata: dict) -> bool:
        if 'identifier' not in translated_metadata:
            return False

        identifier = translated_metadata['identifier']
        if (
                identifier['type'] == 'DOI' and
                'journal.pone' in identifier['value']
                ):
            return True

        return False

    def _contains_invalid_key_value_combinations(
            self, translated_metadata: dict,
            ) -> bool:
        for key, options in self.postfilters.items():
            if key in translated_metadata:
                value = translated_metadata[key]
                if isinstance(options, dict):
                    for k, filterset in options.items():
                        subvalue = value[k]
                        if subvalue in filterset:
                            return True
                elif value in options:
                    return True

        return False

    def post_process(self, metadata: ResourceMetadata):
        if (
                self._has_no_title(metadata.translated) or
                self._has_sparse_metadata(metadata.translated) or
                self._contains_unwanted_type(metadata.translated) or
                self._contains_invalid_title_or_description(
                    metadata.translated
                    ) or
                self._has_plusone_identifier(metadata.translated) or
                self._contains_invalid_key_value_combinations(
                    metadata.translated
                )
                ):
            metadata.is_filtered = True


class Optimizer(PostProcessor):
    """
    Optimizer PostProcessor

    Optimizes translated data, so the relations between fields are correct
    """
    text_formats = set(['PDF', 'TXT', 'DOC', 'DOCX', 'RTF'])

    def _optimize_dates(self, translated_metadata: dict):
        org_created = translated_metadata.get('created')
        if org_created is not None:
            org_issued = translated_metadata.get('issued')
            org_modified = translated_metadata.get('modified')
            if org_issued is not None:
                # The issued date has to be great then or equal to
                # the created date, otherwise it's invalid
                if org_issued < org_created:
                    # Take the lowest. Except if the issued date is the first
                    # of January, indicating it was parsed from a year, the
                    # created date is most likely more detailed
                    if not org_issued.endswith('01-01'):
                        translated_metadata['created'] = org_issued
                        del translated_metadata['issued']
                    else:
                        del translated_metadata['issued']

            if org_modified is not None:
                created = translated_metadata['created']
                if org_modified < created:
                    translated_metadata['created'] = org_modified
                    del translated_metadata['modified']
                    # Restore the 'issued', when created has become lower...
                    if (
                            org_issued is not None and
                            'issued' not in translated_metadata and
                            translated_metadata['created'] < org_issued
                            ):
                        translated_metadata['issued'] = org_issued

    def _optimize_creator_publisher(self, translated_metadata: dict):
        if (
                'creator' in translated_metadata and
                'publisher' in translated_metadata
                ):
            p_name = translated_metadata['publisher']['name'].strip().lower()

            remove_inds = []
            for ind_, c in enumerate(translated_metadata['creator']):
                if 'name' in c:
                    name = c['name']
                else:
                    name = c['organization']

                if name.strip().lower() == p_name:
                    remove_inds.append(ind_)

            if remove_inds != []:
                if len(remove_inds) == len(translated_metadata['creator']):
                    del translated_metadata['creator']
                else:
                    for i in reversed(remove_inds):
                        del translated_metadata['creator'][i]

    def _optimize_document_types(self, translated_metadata: dict):
        """
        If an entry only has text based formats, and the title contains
        specific phrases, it can be classified as a Document or Document:Report
        """
        formats = translated_metadata.get('format')
        title = translated_metadata.get('title')
        if formats is None or title is None:
            return

        has_nontext_formats = bool(set(formats).difference(self.text_formats))
        if has_nontext_formats:
            return

        for phrase in ['paper', 'guideline', 'guidelines', 'poster']:
            if phrase in title:
                translated_metadata['type'] = ['Document']
                return

        if 'report' in title:
            translated_metadata['type'] = ['Document', 'Document:Report']
            return

    def post_process(self, metadata: ResourceMetadata):
        self._optimize_dates(metadata.translated)
        self._optimize_creator_publisher(metadata.translated)
        self._optimize_document_types(metadata.translated)


class Scorer(PostProcessor):
    """
    Scorer PostProcessor

    Adds scores to metadata.meta based on metadata quality (used in ES sorting)
    """
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

    subject_scheme_data = _loadcfg.subject_scheme()

    desc_sc = [[0, 0.20], [400, 1], [1000, 1], [2048, 0.5]]
    title_sc = [[0, 0], [50, 1], [100, 1], [256, 0.5]]

    def _interp_curve(self, x: float, curve: list[list[float]]) -> float:
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

    def _get_description_score(self, translated_metadata: dict) -> float:
        description = translated_metadata.get('description')
        if description is not None:
            # Using a skewed distribution to score length
            return self._interp_curve(len(description), self.desc_sc)
        else:
            return 0

    def _get_title_score(self, translated_metadata: dict) -> float:
        title = translated_metadata['title']
        return self._interp_curve(len(title), self.title_sc)

    def _get_subject_depth(self, subject_id: str) -> int:
        """
        Determine the depth of a subject in the hierarchy (least deep mention)
        """
        s_data = self.subject_scheme_data[subject_id]
        all_parents = s_data['parents'] + s_data['relations']
        if len(all_parents) == 0:
            return 0
        else:
            depths = []
            for parent in all_parents:
                depths.append(self._get_subject_depth(parent))
            min_depth = min(depths)
            return min_depth + 1

    def _get_subject_score(self, translated_metadata: dict) -> float:
        """
        Compound score based on the number of subjects, and how detailed they
        ares
        """
        subject = translated_metadata.get('subject')
        if subject is not None:
            low_level_subjects = subject['low_level']
            # Determine individual subject scores
            s_scores = []
            for subject_id in low_level_subjects:
                depth = self._get_subject_depth(subject_id)
                s_scores.append(self.depth_score[depth])
            avg_score = mean(s_scores)
            # Now score for amount
            amount = len(s_scores)

            # Since the 'low_level' is no longer used, replace subject data
            translated_metadata['subject'] = subject['all']

            return self.amount_score[amount] * avg_score
        else:
            return 0

    def _get_location_score(self, translated_metadata: dict) -> int:
        """
        Binary score based on existence of the location field
        """
        if len(translated_metadata.get('location', [])) > 0:
            return 1
        else:
            return 0

    def _get_dates_score(self, translated_metadata: dict) -> float:
        """
        One date, 0.5, two dates 0.75 and three 1.0
        """
        count = 0
        for date_key in ['modified', 'created', 'issued']:
            if translated_metadata.get(date_key) is not None:
                count += 1

        if count > 0:
            return 0.5 + ((count - 1)/2) * 0.5
        else:
            return 0

    def _get_timeperiod_score(self, translated_metadata: dict) -> int:
        """
        Binary score based on timeperiod field
        """
        if translated_metadata.get('timePeriod') is not None:
            return 1
        else:
            return 0

    def _get_license_score(self, translated_metadata: dict) -> int:
        """
        Scores license name only, since otherwise license data is incomplete
        """
        if translated_metadata.get('license', {}).get('name') is not None:
            return 1
        else:
            return 0

    def post_process(self, metadata: ResourceMetadata):
        scores = {
            'description': self._get_description_score(metadata.translated),
            'title': self._get_title_score(metadata.translated),
            'subject': self._get_subject_score(metadata.translated),
            'location': self._get_location_score(metadata.translated),
            'dates': self._get_dates_score(metadata.translated),
            'timePeriod': self._get_timeperiod_score(metadata.translated),
            'license': self._get_license_score(metadata.translated),
        }

        scores['total'] = (
            0.25 * scores['description'] +
            0.25 * scores['title'] +
            0.15 * scores['subject'] +
            0.15 * scores['location'] +
            0.10 * scores['dates'] +
            0.05 * scores['timePeriod'] +
            0.05 * scores['license']
        )

        metadata.meta['scores'] = scores


class MetadataPostProcessor(PostProcessor):
    """
    MetadataPostProcessor

    Combined PostProcessor that runs the Filter, Optimizer and Scorer or the
    data
    """
    def __init__(self, enable_filters: bool = True) -> None:
        """
        Initializes the MetadataPostProcessor

        Args:
            enable_filters:
                Optional; If False, data is not being filtered
        """
        self._filter = Filter()
        self._optimizer = Optimizer()
        self._scorer = Scorer()
        self.enable_filters = enable_filters

    def post_process(self, metadata: ResourceMetadata):
        if self.enable_filters:
            self._filter.post_process(metadata)
            if metadata.is_filtered:
                return
        self._optimizer.post_process(metadata)
        self._scorer.post_process(metadata)
