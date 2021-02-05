# -*- coding: utf-8 -*-
"""
STRUCTURING SUB-MODULE

Contains functions that structure the ingested metadata into a flattened format
, creating a consistent format before translation of metadata

Every Flattening Function should:
* Filter Junk data
* Create a '_dplatform_externalReference' and '_dplatform_uid'
"""
import re
import logging
import copy

from metadata_ingestion import _dataio, _aux, structurers
from metadata_ingestion import settings as st

logger = logging.getLogger(__name__)

# To reformat geoblacklight keys:
bl_key_replace = re.compile(
    '(^(dct?|layer)_)|(_(sm?|dt|ssim|dtsi|ssi|ssm|tesim|dtsim)$)'
)


def create_dplatform_externalReference(link):
    """
    Create a externalReference instance for the data portal

    Arguments:
        link --- str: Link to resource description on external data portal

    Returns:
        dplatform_externalReference --- dict: externalReference metadata
        instance
    """
    dplatform_externalReference = {
        'URL': link,
        'type': 'synchronizedPortalPage'
        }

    return dplatform_externalReference


ckan3_structurer = structurers.CKANStructurer('dummyid', base_url='')


def CKAN3(data, reference_baseURL='', filter_org_ids=None,
          filter_group_names=None):
    """
    Creates a dict from CKAN data with an attribute/value structure. Raises all
    relevant attributes to parent (flattens)

    Input:
        data --- dict: Resource description of one CKAN dataset

        reference_baseURL='' --- str:The base URL to which the name of the
        dataset has to be appended, in order to get a working link to the data
        portal.

        filter_org_ids=None --- list: If given, data from the organizations
        with these ids will be filtered out

        filter_group_names=None --- list: If given, data that has any of the
        group names in this list, will be filtered out

    Returns:
        flattened Data --- A dictionary with all relevant attribute/value
        combinations
    """
    ckan3_structurer.base_url = reference_baseURL
    ckan3_structurer.key_value_filter_options = filter_options = []
    if filter_org_ids is not None:
        filter_options.append({
            'key': {'organization': 'id'},
            'values': set(filter_org_ids),
            'type': 'reject',
            'should_completely_match': False
        })
    if filter_group_names is not None:
        filter_options.append({
            'key': {'groups': 'name'},
            'values': set(filter_group_names),
            'type': 'reject',
            'should_completely_match': False
        })

    return structure_using_structurer(data, ckan3_structurer)


socrata_structurer = structurers.SocrataStructurer('dummyid')


def socrata_discovery_v1(data, **kwargs):
    """
    Flatten the Socrata Discovery API (v1) output, to raise all metadata to
    parent.

    Input:
        data --- dict: A single resource description retrieved from the API

    Returns:
        flattened_data --- dict: A single flattened resource description
    """
    return structure_using_structurer(data, socrata_structurer)


def create_OAI_PMH_reference(org_id, reference_baseURL, id_prefix=None):
    """
    Create the 'dplatform_externalReference' for an OAI_PMH resource
    description, by removing prefixed text from the 'header:identifier' in
    order to yield the identifier used in the url.

    Arguments:
        org_id --- str: The contents of the 'header:identifier' field

        reference_baseURL --- str: The base url to which the cleaned id should
        be appended

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

    Returns:
        str: The data portal dplatform_externalReference
    """
    if id_prefix is None:
        new_id = re.sub(r'^(.*(?<!:):(?!:))', '', org_id)
    else:
        if not isinstance(id_prefix, list):
            id_prefix = [id_prefix]
        new_id = org_id
        for prefix in id_prefix:
            new_id = new_id.replace(prefix, '')

    # HACK: quickfix for ANDS
    if new_id.startswith('ands.org.au::'):
        new_id = new_id.replace('ands.org.au::', '')

    dplatform_link = reference_baseURL + new_id

    dplatform_externalReference = create_dplatform_externalReference(
        dplatform_link
        )

    return dplatform_externalReference


def process_OAI_PMH_metadata(merged_metadata, reference_baseURL,
                             id_prefix=None):
    """
    Clean the merged metadata from the OAI-PMH API, determine the UID and
    append the link to the platform page

    Arguments:
        merged_metadata --- The entry in which the data under 'metadata' and
        'header' are merged into one

        reference_baseURL --- str: The base URL to determine the url of the
        dataset

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

    Returns:
        dict --- The structured data
    """
    # Clean the data:
    structured_data = _aux.clean_xml_metadata(merged_metadata)

    # Assign the link to the data platform:
    dplatform_uid = structured_data['header:identifier']
    dplatform_externalReference = create_OAI_PMH_reference(dplatform_uid,
                                                           reference_baseURL,
                                                           id_prefix=id_prefix)

    structured_data.update(
        {'_dplatform_externalReference': dplatform_externalReference,
         '_dplatform_uid': dplatform_uid}
        )

    return structured_data


oai_datacite_payload_structurer = structurers.OAIDatacitePayloadStructurer('')


def OAI_PMH_oai_datacite(data, reference_baseURL='', id_prefix=None):
    """
    Flatten parsed XML metadata descriptions that are retrieved in datacite
    format from an OAI-PMH API. supports 'metadata' --> 'oai_datacite' -->
    'payload' structure. See http://schema.datacite.org/oai/oai-1.0/oai.xsd

    Argument:
        data --- dict: The raw parsed data for a single entry

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

    Returns:
        dict --- The flattened & cleaned data
    """
    oai_datacite_payload_structurer.base_url = reference_baseURL
    if id_prefix is not None:
        if isinstance(id_prefix, str):
            id_prefix = [id_prefix]
    oai_datacite_payload_structurer.id_prefix = id_prefix

    return structure_using_structurer(data, oai_datacite_payload_structurer)


oai_datacite_resource_structurer = structurers.OAIDataciteResourceStructurer(
    ''
)


def OAI_PMH_datacite(data, reference_baseURL='', id_prefix=None,
                     keep_types=None):
    """
    Flatten parsed XML metadata descriptions that are retrieved in datacite (3)
    format from an OAI-PMH API. Supports the 'metadata' --> 'resource'
    structure. See http://schema.datacite.org/meta/kernel-3/metadata.xsd

    Argument:
        data --- dict: The raw parsed data for a single entry

        reference_baseURL='' --- str: The URL to append the ID to, to form the
        reference to the resource

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

        keep_types=None --- list[str]: The resourceType/@resourceTypeGeneral
        values to keep. Entries without these are rejected

    Returns:
        dict --- The flattened & cleaned data
    """
    oai_datacite_resource_structurer.base_url = reference_baseURL
    if id_prefix is not None:
        if isinstance(id_prefix, str):
            id_prefix = [id_prefix]
    oai_datacite_resource_structurer.id_prefix = id_prefix

    if keep_types is not None:
        oai_datacite_resource_structurer.key_value_filter_options = [
            {
                'key': {'resourceType': '@resourceTypeGeneral'},
                'values': keep_types,
                'type': 'accept',
                'should_completely_match': False,
                'standalone_accept': True
            },
            {
                'key': {'resourceType': '#text'},
                'values': keep_types,
                'type': 'accept',
                'should_completely_match': False,
                'standalone_accept': True
            },
        ]
        oai_datacite_resource_structurer.last_accept_index = 1
    else:
        oai_datacite_resource_structurer.key_value_filter_options = []

    return structure_using_structurer(data, oai_datacite_resource_structurer)


oai_dc_structurer = structurers.OAIDCStructurer('')


def OAI_PMH_oai_dc(
        data, reference_baseURL='', id_prefix=None, keep_types=None,
        identifier_is_url=False
        ):
    """
    Flatten parsed xml descriptions harvested from an OAI-PMH endpoint in the
    Dublin Core format. http://www.openarchives.org/OAI/2.0/oai_dc.xsd

    Arguments:
        data --- dict: The raw parsed data for a single entry

        reference_baseURL --- str: The base url to create the reference

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

        keep_types=None --- list: Which types to keep when structuring (all
        other types, also missing values, are rejected)

        identifier_is_url=False --- bool: If True, the identifier is used as
        the resource URL

    Returns:
        dict --- The flattened and cleaned data
    """
    oai_dc_structurer.base_url = reference_baseURL

    if id_prefix is not None:
        if isinstance(id_prefix, str):
            id_prefix = [id_prefix]
    oai_dc_structurer.id_prefix = id_prefix

    if keep_types is not None:
        oai_dc_structurer.key_value_filter_options = [
            {
                'key': 'type',
                'values': keep_types,
                'type': 'accept',
                'should_completely_match': False,
                'standalone_accept': False
            },
        ]

        oai_dc_structurer.last_accept_index = 1
    else:
        oai_dc_structurer.key_value_filter_options = []

    if identifier_is_url:
        oai_dc_structurer.url_key = 'identifier'
    else:
        oai_dc_structurer.url_key = None

    return structure_using_structurer(data, oai_dc_structurer)


arcgis_open_data_structurer = structurers.ArcGISOpenDataStructurer(
    '', base_url=''
)


# The * in the below function definition, causes the base_url to be a kw-arg
def arcgis_open_data_v3(data, *, base_url):
    """
    Flatten the metadata structure of a single resource description in ARCGIS
    Open Data format (v3)

    Input:
        data --- dict: The metadata of a single resource description

        base_url --- str: The base URL for references to datasets

    Returns:
        dict --- The flattened metadata of a single resource description
    """
    arcgis_open_data_structurer.base_url = base_url

    return structure_using_structurer(data, arcgis_open_data_structurer)


knoema_dcat_structurer = structurers.KnoemaDCATStructurer('')


def knoema_dcat(data, **kwargs):
    """
    Flatten the metadata structure of a single resource description in the DCAT
    (Knoema) format

    Arguments:
        data --- dict: The raw metadata from the API

    Returns:
        dict --- The flattened data
    """
    return structure_using_structurer(data, knoema_dcat_structurer)


opendatasoft_structurer = structurers.OpenDataSoftStructurer('', base_url='')


def opendatasoft_v1(data, reference_baseURL='', **kwargs):
    """
    Flatten the metadata structure of a single opendatasoft resource
    description

    Arguments:
        data --- dict: The raw metadata from the API

    Returns:
        dict --- The flattened data
    """
    opendatasoft_structurer.base_url = reference_baseURL
    return structure_using_structurer(data, opendatasoft_structurer)


oai_iso_19139_structurer = structurers.OAIISO19139Structurer(
    '',
    base_url=''
)


def OAI_PMH_iso19139(data, base_url='', id_prefix=None):
    """
    Flatten parsed XML metadata descriptions that are retrieved in ISO19139
    format from an OAI-PMH API.

    Argument:
        data --- dict: The raw parsed data for a single entry

        base_url='' --- str: The base url used to create the links to the data
        portal

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

    Returns:
        dict --- The flattened & cleaned data
    """
    oai_iso_19139_structurer.base_url = base_url

    if id_prefix is not None:
        if isinstance(id_prefix, str):
            id_prefix = [id_prefix]
    oai_iso_19139_structurer.id_prefix = id_prefix

    return structure_using_structurer(data, oai_iso_19139_structurer)


_dataverse_structurer = structurers.DataverseStructurer(
    '', base_url=''
)


def dataverse(data, reference_baseURL=''):
    """
    Structure the metadata of a single dataverse catalog resource
    """
    _dataverse_structurer.base_url = reference_baseURL
    return structure_using_structurer(data, _dataverse_structurer)


dataverse_schema_org_structurer = structurers.DataverseSchemaOrgStructurer(
    '', base_url=''
)


def dataverse_schema_org(data, reference_baseURL=''):
    """
    Structure metadata from a dataverse resource if the schema.org exporter is
    used. This adds formats based on the distributions
    """
    dataverse_schema_org_structurer.base_url = reference_baseURL
    return structure_using_structurer(data, dataverse_schema_org_structurer)


geonode_structurer = structurers.GeonodeStructurer('', base_url='')


def geonode(data, base_url='', exclude_prefixes=None):
    """
    Structure the metadata of a single geonode resource

    Arguments:
        data --- dict: The data to structure

        base_url='' --- str: the URL of the platform, used to create the link
        to a resource

        exclude_prefixes=None --- list: A list of name prefixes to exclude.
        These prefixes are used to identify data harvested from external
        services
    """
    geonode_structurer.base_url = base_url.rstrip('/')
    geonode_structurer.exclude_prefixes = exclude_prefixes

    return structure_using_structurer(data, geonode_structurer)


csw_structurer = structurers.CSWStructurer('', base_url='')


def CSW2(
        data, base_url, remove_keys=None, remove_types=None,
        reverse_corner_coordinates=False, id_to_lower=False):
    """
    Structuring function for CSW2 data

    Arguments:
        data --- dict: The data for a single entry

        base_url --- str: The url that's prefixed to identifiers, to create a
        reference to the dataset

        remove_keys=None --- list[str]: A list of the keys that should be
        removed in structuring

        remove_types=None --- list[str]: Entries with these values in the
        'type' field, are rejected

        reverse_corner_coordinates=False --- bool: If True, the
        BoundingBox.LowerCorner and UpperCorner attributes are reversed in
        order. Use this for garbage Y,X formats.

        id_to_lower=False --- str: If true, the id is converted to lowercase.
        In some cases (e.g. geonetwork) this may be required for all links to
        work.
    """
    csw_structurer.base_url = base_url
    csw_structurer.remove_keys = remove_keys
    csw_structurer.reverse_corner_coordinates = reverse_corner_coordinates
    csw_structurer.id_to_lower = id_to_lower

    key_value_filter_options = []
    if remove_types:
        key_value_filter_options = [
            {
                'key': 'type',
                'values': remove_types,
                'type': 'reject',
                'should_completely_match': False
            }
        ]
    csw_structurer.key_value_filter_options = key_value_filter_options

    return structure_using_structurer(data, csw_structurer)


gmd_structurer = structurers.GMDStructurer('', base_url='')


def gmd(data, base_url, remove_ids_containing=None):
    """
    Structuring function for CSW2 data

    Arguments:
        data --- dict: The data for a single entry

        base_url --- str: The url that's prefixed to identifiers, to create a
        reference to the dataset

        remove_ids_containing --- list[str]: If the id contains one of these
        phrases, discard the entry
    """
    gmd_structurer.base_url = base_url
    gmd_structurer.remove_ids_containing = remove_ids_containing

    return structure_using_structurer(data, gmd_structurer)


dataone_structurer = structurers.DataOneStructurer('', base_url='')


def dataone(data, base_url):
    """
    Structuring function for DataONE data

    Arguments:
        data --- dict: The data of a single entry

        base_url --- str: The base url, to which the id of the entry is
        appended, to form the link to the resource
    """
    dataone_structurer.base_url = base_url

    return structure_using_structurer(data, dataone_structurer)


blacklight_structurer = structurers.BlackLightStructurer(
    '', base_url='', id_key=''
)


def blacklight(data, base_url, uid_key, remove_keys=None, url_suffix_key=None,
               url_suffix_mapping=None, raise_key=None):
    """
    Structure blacklight data

    Arguments:
        data --- dict: The data of a single entry

        base_url --- str: The URL to which to base references to resources on

        uid_key --- str: The key to use for the URL and unique identifier

        remove_keys=None --- list: If given, these keys will be removed before
        translation (usefull if they get mistranslated)

        url_suffix_key=None --- str: If provided, this key's value (or first
        value from list) is used to lookup a suffix in the url_suffix_mapping

        url_suffix_mapping=None --- dict: If provided, together with
        url_suffix_key, this uses the value of the url_suffix_key to lookup the
        correct string that should be appended to the base_url

        raise_key=None --- str: If defined, the data under this key (if its a
        dict) will be raised to parent level
    """
    blacklight_structurer.base_url = base_url
    blacklight_structurer.id_key = uid_key
    blacklight_structurer.remove_keys = remove_keys
    blacklight_structurer.base_url_suffix_mapping_key = url_suffix_key
    blacklight_structurer.base_url_suffix_mapping = url_suffix_mapping
    update_from_keys = []
    if raise_key is not None:
        update_from_keys = [raise_key]
    blacklight_structurer.update_from_keys = update_from_keys

    return structure_using_structurer(data, blacklight_structurer)


simple_structurer = structurers.SimpleStructurer('', base_url='', id_key='')


def simple(data, *, base_url, append_key, uid_key, url_remove=None):
    """
    A simple structurer, that does not touch the data, but only takes the
    given base_url and appends the data under the 'append_key' to it (for
    example 'id' key).

    Arguments:
        data --- dict: The data of a single entry

        base_url --- str: The URL to which the data from the 'append_key'
        should be appended

        append_key --- str: The key of which the corresponding value should
        be appended to base_url, in order to retrieve the url of the resource

        uid_key --- str: The key that represents the unique identifier of the
        entry

        url_remove=None --- str: A phrase that should be replaced by '' in the
        url
    """
    simple_structurer.url_suffix_key = None
    if base_url == '':
        simple_structurer.base_url = None
        simple_structurer.url_key = append_key
    else:
        simple_structurer.base_url = base_url
        simple_structurer.url_key = None
        if append_key != uid_key:
            simple_structurer.url_suffix_key = append_key

    simple_structurer.id_key = uid_key
    simple_structurer.remove_from_url = url_remove

    return structure_using_structurer(data, simple_structurer)


data_gov_in_structurer = structurers.DataGovINStructurer('', base_url='')


def data_gov_in(data, base_url=''):
    """
    Structurer for data harvested from the data.gov.in API

    Arguments:
        data --- dict: The data of a single entry

        base_url --- str: The base url for resource links
    """
    data_gov_in_structurer.base_url = base_url.strip('/')

    return structure_using_structurer(data, data_gov_in_structurer)


sciencebase_structurer = structurers.ScienceBaseStructurer('')


def sciencebase(data):
    """
    Structurer for data harvested from ScienceBase.gov

    Arguments:
        data --- dict: The data of a single entry
    """
    return structure_using_structurer(data, sciencebase_structurer)


geoplatform_structurer = structurers.GeoplatformStructurer('')


def geoplatform(data, base_url=''):
    """
    Structurer for data harvested from GeoPlatform.gov

    Arguments:
        data --- dict: The data of a single entry
    """
    geoplatform_structurer.base_url = base_url
    return structure_using_structurer(data, geoplatform_structurer)


elasticsearch_structurer = structurers.ElasticSearchStructurer('')


def elasticsearch(data, base_url=''):
    """
    Structurer for data harvested from an ElasticSearch endpoint
    """
    elasticsearch_structurer.base_url = base_url
    return structure_using_structurer(data, elasticsearch_structurer)


invenio_structurer = structurers.InvenioStructurer('')


def invenio(data, base_url=''):
    """
    Structurer for data harvested from an Invenio API endpoint
    """
    invenio_structurer.base_url = base_url
    return structure_using_structurer(data, invenio_structurer)


ncei_structurer = structurers.NCEIStructurer('')


def ncei(data):
    """
    Structurer for NCEI data
    """
    return structure_using_structurer(data, ncei_structurer)


magda_structurer = structurers.MagdaStructurer('')


def magda(data, url_format='{}', filter_publishers=None):
    """
    Structurer for data harvested from a Magda.io API endpoint

    Arguments:
        data --- dict: The data of a single record harvested from the Magda API

        url_format --- str: A format string, with one '{}', where the url
        encoded id of the entry is placed

        filter_publishers=None --- list: If provided, any entry with a
        dataset-publisher aspect that has one of the id's in this list, is
        filtered
    """
    magda_structurer.format_url = url_format
    if filter_publishers is not None:
        magda_structurer.filter_key_value_options = [
            {
                'key': {'aspects': {'dataset-publisher': 'publisher'}},
                'values': filter_publishers,
                'type': 'reject',
                'should_completely_match': False,
            }
        ]
    else:
        magda_structurer.filter_key_value_options = []

    return structure_using_structurer(data, magda_structurer)


rifcs_structurer = structurers.RIFCSStructurer('')


def RIF_CS(data, base_url='', id_prefix=None):
    """
    Flatten parsed XML metadata descriptions that are retrieved in datacite
    format from an OAI-PMH API. supports 'metadata' --> 'oai_datacite' -->
    'payload' structure. See http://schema.datacite.org/oai/oai-1.0/oai.xsd

    Argument:
        data --- dict: The raw parsed data for a single entry

        base_url --- str: The base url for links to the resource

        id_prefix=None --- str/list: If given, this will be replaced in the id
        to determine the url. Provide a list if a source has more than 1
        possible prefix. If it is None, by default everything before the
        colon is removed

    Returns:
        dict --- The flattened & cleaned data
    """
    if isinstance(id_prefix, str):
        id_prefix = [id_prefix]
    rifcs_structurer.id_prefix = id_prefix
    rifcs_structurer.base_url = base_url

    return structure_using_structurer(data, rifcs_structurer)


def geonetwork(data, base_url='', id_prefix=None):
    """
    Structure data returned by the Geonetwork API (Not for geonetwork CSW data)

    Argument:
        data --- dict: The raw parsed data for a single entry

        base_url --- str: The url to which the resource UUID is appended to
        create the URL

    Returns:
        dict --- The flattened & cleaned data
    """
    structured_metadata = copy.deepcopy(data)
    geonetwork_meta = data.pop('geonet:info')

    uuid = geonetwork_meta['uuid']

    dataset_url = base_url.strip('/') + '/' + uuid
    ext_reference = create_dplatform_externalReference(dataset_url)
    structured_metadata.update(
        {'_dplatform_externalReference': ext_reference,
         '_dplatform_uid': uuid}
        )

    return structured_metadata


def EUDP(data, base_url='', filter_catalog_ids=None):
    """
    Structure data harvested from the European Data Portal

    Arguments:
        data --- dict: The data of a single entry

        base_url='' --- str: The base url of the links to the website

        filter_catalog_ids --- list[str]: The id's of the catalogs that should
        be filtered from the results

    Returns:
        dict --- The structured data of a single entry
    """
    structured_metadata = copy.deepcopy(data)

    # Check if catalog id should be filtered
    if filter_catalog_ids is not None:
        if structured_metadata['catalog']['id'] in filter_catalog_ids:
            return

    dataset_id = structured_metadata.pop('id')

    dataset_url = base_url.strip('/') + '/' + dataset_id
    ext_reference = create_dplatform_externalReference(dataset_url)
    structured_metadata.update(
        {'_dplatform_externalReference': ext_reference,
         '_dplatform_uid': dataset_id}
        )

    # Extract data formats
    if 'format' not in structured_metadata:
        formats = []
        distributions = structured_metadata.pop('distributions', None)
        if distributions is not None:
            for dist in distributions:
                dformat = dist.get('format')
                if isinstance(dformat, dict) and 'id' in dformat:
                    formats.append(dformat['id'])

        formats = list(set(formats))
        if len(formats) > 0:
            structured_metadata['format'] = formats

    # If it doesn't have a type, add type dataset
    # (Earlier harvesting enpoint assigned dataset to all entries)
    if 'type' not in structured_metadata:
        structured_metadata['type'] = 'Dataset'

    return structured_metadata


_junar_structurer = structurers.JunarStructurer('')


def junar(data):
    """
    Structure data harvested from a Junar API (v2)

    Arguments:
        data --- dict: The data of a single entry

    Returns:
        dict --- The structured data of a single entry
    """
    return structure_using_structurer(data, _junar_structurer)


def udata(data, base_url=None):
    """
    Structure data harvested from a Udata API (v1)

    Arguments:
        data --- dict: The data of a single entry

    Returns:
        dict --- The structured data of a single entry

        base_url=None --- str: If given, this is used as the base url, and is
        suffixed with the 'slug' to form the link. If not given, the defaul
        'page' value is used. (Note that this is usefull if the default page
        links to a different language version of the webpage)
    """
    structured_metadata = copy.deepcopy(data)

    dataset_id = structured_metadata.pop('id')
    slug = structured_metadata.pop('slug')
    dataset_url = structured_metadata.pop('page')
    if base_url:
        dataset_url = base_url.strip('/') + '/' + slug

    ext_reference = create_dplatform_externalReference(dataset_url)
    structured_metadata.update(
        {'_dplatform_externalReference': ext_reference,
         '_dplatform_uid': dataset_id}
        )

    # Get the formats from the resources list
    resources = structured_metadata.pop('resources', [])
    formats = set()
    for resource in resources:
        format = resource.get('format')
        if format:
            formats.add(format)
    if formats:
        structured_metadata['format'] = list(formats)

    return structured_metadata


def data_json(data):
    """
    Structurer for data.json data

    Arguments:
        data --- dict: The data of a single entry
    """
    structured = copy.deepcopy(data)
    dataset_url = dataset_id = data.pop('identifier')

    # Add the data to the entry
    ext_reference = create_dplatform_externalReference(dataset_url)
    structured.update(
        {'_dplatform_externalReference': ext_reference,
         '_dplatform_uid': dataset_id}
        )

    # Rename type key and get formats from distributions
    type_ = structured.pop('@type')
    if type_:
        structured['type'] = type_

    dist_data = structured.pop('distribution')
    formats = []
    if dist_data:
        if isinstance(dist_data, dict):
            dist_data = [dist_data]
        if isinstance(dist_data, list):
            for dist in dist_data:
                format = dist.get('format')
                if format:
                    formats.append(format)
    if formats:
        structured['format'] = formats

    return structured


def dcat_xml(data):
    """
    Structuring function for DCAT XML parsed data

    Arguments:
        data --- dict: The data for a single entry
    """
    cleaned = _aux.clean_xml_metadata(data, prefer_upper=True)

    structured = copy.deepcopy(cleaned['Dataset'])

    # Create the URL:
    dataset_id = structured.pop('identifier', None)
    dataset_url = structured.pop('about', None)
    if dataset_id is None or dataset_url is None:
        # Should be very rare
        return

    # Add the data to the entry
    ext_reference = create_dplatform_externalReference(dataset_url)
    structured.update(
        {'_dplatform_externalReference': ext_reference,
         '_dplatform_uid': str(dataset_id)}
        )

    return structured


STRUCTURING_LOOKUP = {
    'CKAN3': CKAN3,
    'socrata_discovery_v1': socrata_discovery_v1,
    'oai_datacite': OAI_PMH_oai_datacite,
    'non_oai_datacite': OAI_PMH_datacite,
    'oai_dc': OAI_PMH_oai_dc,
    'arcgis_open_data_v3': arcgis_open_data_v3,
    'knoema': knoema_dcat,
    'opendatasoft_v1': opendatasoft_v1,
    'dataverse': dataverse,
    'dataverse_schema_org': dataverse_schema_org,
    'geonode': geonode,
    'CSW2': CSW2,
    'blacklight': blacklight,
    'DataONE': dataone,
    'simple': simple,
    'DataGovIN': data_gov_in,
    'ScienceBase': sciencebase,
    'GeoPlatform': geoplatform,
    'ElasticSearch': elasticsearch,
    'Invenio': invenio,
    'Magda': magda,
    'RIF-CS': RIF_CS,
    'GeoNetwork': geonetwork,
    'EUDP': EUDP,
    'OAI_PMH_ISO19139': OAI_PMH_iso19139,
    'Junar': junar,
    'GMD': gmd,
    'Udata': udata,
    "Data.json": data_json,
    "DCAT XML": dcat_xml,
    "NCEI": ncei,
    }


def single_entry(data, data_format, **kwargs):
    """
    Structure a single entry with the specified metadata format

    Arguments:
        data --- dict: The metadata of a single entry/line

        data_format --- str: The data format

        **kwargs --- dict: Additional keyword arguments for specific
        structuring functions

    Returns:
        dict --- The structured entry
    """
    return STRUCTURING_LOOKUP[data_format](data, **kwargs)


def ingested_data(in_fileloc, out_fileloc, data_format, **kwargs):
    """
    Flatten all entries of a json-lines file, and write the result to a new
    json-lines file.

    Input:
        in_fileloc --- str: Location of input JSON-lines file with ingested
        metadata

        out_fileloc --- str: Location of output JSON-lines file with flattened
        metadata

        data_format --- str: The format of the data to be translated

        **kwargs --- Additional keyword arguments specific to each data type
        (see individual flatten functions)

    Output:
        JSON-lines file --- Contains the flattened metadata of each entry in
        the input data
    """
    _dataio.remove_if_exists(out_fileloc)

    try:
        flattened_entries = []
        for entry_data in _dataio.iterate_jsonlines(in_fileloc):
            flattened_entry = single_entry(entry_data, data_format, **kwargs)
            flattened_entries.append(flattened_entry)
            flattened_entries = _dataio.save_queue_on_exceedance(
                flattened_entries,
                out_fileloc,
                st.WRITE_PER
                )

        else:
            _dataio.savejsonlines(flattened_entries, out_fileloc, mode='a')
    except Exception as e:
        logger.exception('Structuring {} failed:'.format(in_fileloc))
        raise e


def structure_using_structurer(data, structurer):
    """
    Structure the provided harvested data, using the given structurer
    """
    # Using structurers is the new way, function based is legacy:
    metadata = structurer.structure(data)
    if metadata is not None:
        metadata.add_structured_legacy_fields()
        return metadata.structured
    else:
        return None
