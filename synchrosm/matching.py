"""Tools that help matching objects to OSM data."""

import logging
from math import radians, cos, sin, asin, sqrt
from typing import List, Dict, Union

import rtreelib as r

from synchrosm import database
from synchrosm.models import Node, NodeIdMapping

# Create a custom logger
logger = logging.getLogger('Matching')


def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in meters using havesine formula."""

    radius = 6372800  # meters

    latitude_difference = radians(lat2 - lat1)
    longitude_difference = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(latitude_difference / 2) ** 2 + cos(lat1) * cos(lat2) * sin(longitude_difference / 2) ** 2
    c = 2 * asin(sqrt(a))

    return radius * c


def match_objects(
        objects: List[Dict],
        nodes: List[Node],
        tag_with_id: str = 'ref',
        rtree_search_box: float = 0.001,
        store_in_db: str = None
) -> List[Union[Node, None]]:
    """Match provided objects to OSM nodes stored in the database.
    Returns list that for every input object provides either a matching object or None if no match was found.
    It can match by ID or location."""

    matched_nodes = []

    lookup_dict = {}
    if tag_with_id:
        for node in nodes:
            if node.tags.get(tag_with_id):
                lookup_dict[node.tags.get(tag_with_id)] = node

    t = r.RTree()
    for node in nodes:
        t.insert(
            node,
            r.Rect(node.longitude - 0.0001, node.latitude - 0.0001, node.longitude + 0.0001, node.latitude + 0.0001)
        )

    for obj in objects:
        if lookup_dict:
            if lookup_dict.get(obj.get('id')):
                matched_nodes.append(lookup_dict.get(obj.get('id')))
                continue
        search_area = r.Rect(
            obj.get('longitude') - rtree_search_box,
            obj.get('latitude') - rtree_search_box,
            obj.get('longitude') + rtree_search_box,
            obj.get('latitude') + rtree_search_box
        )
        results = list(t.query(search_area))

        # if we already tried looking for node using some tag with id
        # then location search shouldn't include nodes with this id in tags
        if tag_with_id:
            results = [x for x in results if tag_with_id not in x.data.tags.keys()]

        if len(results) > 0:
            nearest_neighbour = sorted(
                results,
                key=lambda x: distance(obj.get('latitude'), obj.get('longitude'), x.data.latitude, x.data.longitude)
            )[0]
            matched_nodes.append(nearest_neighbour.data)
            continue
        matched_nodes.append(None)

    logger.info(f'Matched {len([x for x in matched_nodes if x is not None])} objects out of {len(objects)}.')

    if store_in_db:
        db = database.DB(store_in_db)
        mappings = [NodeIdMapping(n.id, o.get('id')) for n, o in zip(matched_nodes, objects) if n is not None]
        db.upsert_node_id_mappings(mappings)

    return matched_nodes
