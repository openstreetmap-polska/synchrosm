"""Contains tools that help with downloading data from Overpass API."""

from typing import Dict, Union, List, Generator, Tuple, Optional
import logging
from datetime import datetime

from synchrosm.models import Node

import requests

# Create a custom logger
logger = logging.getLogger('Overpass API')

DEFAULT_OVERPASS_SERVER: str = 'https://lz4.overpass-api.de/api/interpreter'

JSON_VALUE_TYPE = Union[str, int, float, Dict[str, Union[str, int, float]]]
ELEMENT_RAW_TYPE = Dict[str, JSON_VALUE_TYPE]


def osm_nodes(raw_elements: List[ELEMENT_RAW_TYPE]) -> Generator[Node, None, None]:
    """Converts dictionaries to Node class instances.
    Returns a generator so use results in a for loop or convert to list.
    Expects dictionary structure as returned by Overpass API."""

    for element in raw_elements:
        tags = element.get('tags')
        metadata = {
            'changeset': element.get('changeset'),
            'user': element.get('user'),
            'uid': element.get('uid'),
            'timestamp': element.get('timestamp')
        }
        if element.get('type') == 'node':
            yield Node(
                element.get('id'),
                element.get('version'),
                element.get('lat'),
                element.get('lon'),
                tags,
                metadata
            )


def query_from_file(input_filepath: str) -> str:
    """Read Overpass query from file."""

    logger.info(f'Reading query_from_file from: {input_filepath}')
    with open(input_filepath, 'r', encoding='utf-8') as f:
        return f.read()


def call_overpass_api(
        input_filepath: Optional[str] = None,
        query: Optional[str] = None,
        overpass_server: str = DEFAULT_OVERPASS_SERVER
) -> Tuple[datetime, List[ELEMENT_RAW_TYPE]]:
    """Calls Overpass API and returns tuple of OSM base timestamp and list of OSM elements as dictionaries."""

    if input_filepath is None and query is None:
        raise ValueError('You need to specify either filepath to a file with query or provide string with a query.')

    if query is None:
        query = query_from_file(input_filepath)

    logger.info(f'Sending request to: {overpass_server}')

    try:
        response = requests.post(overpass_server, data={'data': query})
        logger.info(f'Received response from: {overpass_server}')
        logger.debug(response.status_code)
        logger.debug(response.url)
        logger.debug(response.headers)
        logger.debug(response.text)
    except Exception as e:
        logger.error('Error for query_from_file in:', input_filepath, exc_info=True)
        raise

    if response.ok:
        data = response.json()
        return (
            datetime.fromisoformat(data.get('osm3s').get('timestamp_osm_base').replace('Z', '+00:00')),
            data.get('elements')
        )
    else:
        logger.error('Request was not successful.')
        logger.error(response.status_code)
        logger.error(response.text)
