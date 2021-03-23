"""Contains helper methods for working with OSM API through OsmApi package."""

import logging
from typing import List, Dict, Any

from synchrosm.models import Node

from osmapi import OsmApi


# Create a custom logger
logger = logging.getLogger('OSM API')


def _split_list_into_chunks(x: List[Any], chunk_size: int = 50) -> List[List[Any]]:
    """Helper function that splits a list into chunks."""

    chunks = [x[i:i + chunk_size] for i in range(0, len(x), chunk_size)]
    return chunks


def list_nodes_as_dict(list_of_ids: List[int], **kwargs) -> Dict[int, Dict[str, Any]]:
    """Get a list of Nodes from OSM API. Returns a dictionary with Node id as key.
    Any additional parameters will be passed to OsmApi class."""

    api = OsmApi(**kwargs)
    results = {}

    chunks = _split_list_into_chunks(list_of_ids)
    logger.info(f'Split input into {len(chunks)} chunks.')

    for chunk_number, chunk in enumerate(chunks):
        logger.info(f'Sending request to OSM API for chunk {chunk_number+1} out of {len(chunks)}.')
        response = api.NodesGet(chunk)
        results = {**results, **response}

    return results


def list_nodes_as_classes(list_of_ids: List[int], **kwargs) -> List[Node]:
    """Get a list of Nodes from OSM API. Returns a list of Node class instances.
    Any additional parameters will be passed to OsmApi class."""

    api = OsmApi(**kwargs)
    results = []

    chunks = _split_list_into_chunks(list_of_ids)
    logger.info(f'Split input into {len(chunks)} chunks.')

    for chunk_number, chunk in enumerate(chunks):
        logger.info(f'Sending request to OSM API for chunk {chunk_number+1} out of {len(chunks)}.')
        response = api.NodesGet(chunk)
        for n in response:
            results.append(
                Node(n['id'], n['version'], n['lat'], n['lon'], n['tag'], {
                    'changeset': n['changeset'],
                    'visible': n['visible'],
                    'timestamp': n['timestamp'],
                    'user': n['user'],
                    'uid': n['uid'],
                })
            )

    return results


def update_nodes(list_of_nodes: List[Node], changeset_tags: Dict[str, str], **kwargs) -> List[Node]:
    """Update Nodes in OSM. Requires a list of Nodes to be updated, changeset tags, and credentials to OsmApi.
    Credentials can be provided in any way that OsmApi supports i.e. user+password, password file etc.
    Pass these parameters as you would pass them to OsmApi."""

    results = []
    api = OsmApi(**kwargs)
    api.ChangesetCreate(changeset_tags)
    for node in list_of_nodes:
        n = api.NodeUpdate(node.as_api_dict())
        results.append(Node(n['id'], n['version'], n['lat'], n['lon'], n['tag'], {'changeset': n['changeset']}))
    api.ChangesetClose()

    return results


def create_nodes(list_of_dicts: List[Dict[str, Any]], changeset_tags: Dict[str, str], **kwargs) -> List[Node]:
    """Create Nodes in OSM. Requires a list of Nodes to be updated, changeset tags, and credentials to OsmApi.
    Credentials can be provided in any way that OsmApi supports i.e. user+password, password file etc.
    Pass these parameters as you would pass them to OsmApi."""

    results = []
    api = OsmApi(**kwargs)
    api.ChangesetCreate(changeset_tags)
    for node in list_of_dicts:
        n = api.NodeCreate(node)
        results.append(Node(n['id'], n['version'], n['lat'], n['lon'], n['tag'], {'changeset': n['changeset']}))
    api.ChangesetClose()

    return results
