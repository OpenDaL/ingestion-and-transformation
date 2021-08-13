# -*- coding: utf-8
"""
Contains the resource classes
"""
import copy
import hashlib


class ResourceMetadata:
    """
    Package for storing the harvested, structured and translated metadata
    throughout processing

    Arguments:
        harvested --- dict: The harvested data

        source_id --- str: The source_id for the harvested data
    """

    def __init__(self, harvested):
        self.harvested = copy.deepcopy(harvested)
        self.structured = {}
        self.translated = {}
        self.is_filtered = False  # can be set in any step of the process
        self.meta = {
            'source': {}
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

    def get_global_id(self):
        source_id = self.meta['source']['id']
        local_id = self.meta['localId']
        encoded_local_id = local_id.encode('utf8')
        if len(encoded_local_id) > 256:
            id_hash = hashlib.md5(encoded_local_id).hexdigest()
            return f'{source_id}-MD5Hash-{id_hash}'
        else:
            return f'{source_id}-{local_id}'

    def get_full_data(self):
        header_metadata = {
            'id': self.get_global_id(),
            'externalReference': {
                'type': 'synchronizedPortalPage',
                'URL': self.meta['url']
            },
            '_source_id': self.meta['source']['id'],
            '_metadata_scores': self.meta['scores'],
        }
        return {**header_metadata, **self.translated}
