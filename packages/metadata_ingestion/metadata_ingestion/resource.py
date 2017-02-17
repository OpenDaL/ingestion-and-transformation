# -*- coding: utf-8
"""
Contains the resource classes
"""
import copy


class ResourceMetadata:
    """
    Package for storing the harvested, structured and translated metadata
    throughout processing

    Arguments:
        harvested --- dict: The harvested data

        source_id --- str: The source_id for the harvested data
    """

    def __init__(self, harvested, source_id):
        self.harvested = copy.deepcopy(harvested)
        # Below can be used for debugging, making sure there's an original
        # self.harvested_original = copy.deepcopy(harvested)
        self.structured = {}
        self.translated = {}
        self.is_filtered = False  # can be set in any step of the process
        self.meta = {
            'source': {
                'id': source_id
            }
        }

    def add_structured_legacy_fields(self):
        """
        Method to recreate the legacy format, to use in old structuring and
        translation functions
        """
        self.structured['_dplatform_externalReference'] = {
            'type': 'synchronizedPortalPage',
            'URL': self.meta['url']
            }
        self.structured['_dplatform_uid'] = self.meta['localId']

    def validate_structured():
        """
        Function to validate whether all the required meta components were
        added after structuring, and whether structured is not empty
        """
        # TODO
        raise NotImplementedError()

    def validate_translated():
        """
        Validate according to the metadata schema. Should be called after
        translation was run
        """
        # TODO
        raise NotImplementedError()
