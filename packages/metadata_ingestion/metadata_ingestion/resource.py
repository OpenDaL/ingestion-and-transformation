# -*- coding: utf-8
"""
Module with the Resource Metadata class

This contains the ResourceMetadata class, which is used throughout the
processing pipeline
"""
import copy
import hashlib


class ResourceMetadata:
    """
    ResourceMetadata class

    Used to store the harvested, structured and translated metadata, as well
    as any other metadata, such as the id of the source portal

    Attributes:
        harvested:
            The harvested data dictionary
        structured:
            The dictionary containing the structured data
        translated:
            The dictionary containing the translated data
        meta:
            Dictionary containing additional metadata, like the source.id
        is_filtered:
            Boolean indicating if the metadata should be filtered
    """

    def __init__(self, harvested: dict):
        """
        Initializes the ResourceMetadata instance

        Args:
            harvested:
                The harvested data
        """
        self.harvested = copy.deepcopy(harvested)
        self.structured = {}
        self.translated = {}
        self.is_filtered = False  # can be set in any step of the process
        self.meta = {
            'source': {}
        }

    @property
    def _global_id(self) -> str:
        """
        Returns the global (system-wide) id, by combining the source id and the
        localId (id in the source portal)
        """
        source_id = self.meta['source']['id']
        local_id = self.meta['localId']
        encoded_local_id = local_id.encode('utf8')
        if len(encoded_local_id) > 256:
            id_hash = hashlib.md5(encoded_local_id).hexdigest()
            return f'{source_id}-MD5Hash-{id_hash}'
        else:
            return f'{source_id}-{local_id}'

    def get_full_data(self) -> dict:
        """
        Returns the full data to be saved to the database

        Call this function after translation
        """
        header_metadata = {
            'id': self._global_id,
            'externalReference': {
                'type': 'synchronizedPortalPage',
                'URL': self.meta['url']
            },
            '_source_id': self.meta['source']['id'],
            '_metadata_scores': self.meta['scores'],
        }
        return {**header_metadata, **self.translated}
