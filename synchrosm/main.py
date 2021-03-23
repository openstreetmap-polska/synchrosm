"""Main interface."""

import logging
import os
from typing import Dict, List, Any

from synchrosm import database, overpass_api, osm_api
from synchrosm.matching import match_objects
from synchrosm.models import Node, ComparisonResults, NodeComparison

# set up logs format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a custom logger
logger = logging.getLogger('Main')


def download_and_store_data(query_or_path: str, db: database.DB = None) -> None:
    """Downloads data from Overpass API and stores results in sqlite database. Supports only Nodes."""

    if db is None:
        db = database.DB()

    if os.path.isfile(query_or_path):
        ts, response = overpass_api.call_overpass_api(input_filepath=query_or_path)
    else:
        ts, response = overpass_api.call_overpass_api(query=query_or_path)
    nodes = overpass_api.osm_nodes(response)

    db.upsert_nodes(nodes)
    db.set_osm_base_timestamp(ts)


def compare_db_data_to_api(
        db_file_path: str = database.DEFAULT_DB_FILE_PATH,
        update_data_in_db: bool = False
) -> ComparisonResults:
    """Compares data in sqlite to OSM API and returns which elements changed and which didn't. Supports only Nodes."""

    results = ComparisonResults()
    db = database.DB(db_file_path)

    nodes_in_db = db.select_nodes(300)

    nodes_ids = [n.id for n in nodes_in_db]
    nodes_in_osm = osm_api.list_nodes_as_dict(nodes_ids)

    for dbnode in nodes_in_db:
        osmnode = nodes_in_osm.get(dbnode.id)
        if osmnode and osmnode['version'] == dbnode.version:
            results.unchanged.append(dbnode)
        elif osmnode and osmnode['version'] > dbnode.version:
            results.new_version_in_osm.append(
                NodeComparison(
                    old=dbnode,
                    new=Node(
                        osmnode.get('id'),
                        osmnode.get('version'),
                        osmnode.get('lat'),
                        osmnode.get('lon'),
                        osmnode.get('tag'),
                        {
                            'visible': osmnode.get('visible'),
                            'changeset': osmnode.get('changeset'),
                            'timestamp': osmnode.get('timestamp'),
                            'user': osmnode.get('user'),
                            'uid': osmnode.get('uid'),
                        }
                    )
                )
            )
        elif osmnode and osmnode['version'] < dbnode.version:
            logger.error(
                f'Node: {dbnode.id} has version: {dbnode.version} while API sent version: {osmnode["version"]}.'
            )
        else:
            logger.error(f'Node: {dbnode.id} not found.')

    logger.info(f'{len(results.unchanged)} nodes did not change.')
    logger.info(f'{len(results.new_version_in_osm)} nodes have newer versions available.')

    if update_data_in_db and len(results.new_version_in_osm) > 0:
        nodes_to_update = [x.new for x in results.new_version_in_osm if all([x.new.latitude, x.new.longitude])]
        nodes_to_delete = [x.new.id for x in results.new_version_in_osm if not all([x.new.latitude, x.new.longitude])]
        if len(nodes_to_update) > 0:
            db.upsert_nodes(nodes_to_update)
        if len(nodes_to_delete) > 0:
            db.delete_nodes(nodes_to_delete)

    return results


def import_data(
        data: List[Dict[str, Any]],
        overpass_query_or_path: str,
        db_file_path: str = None,
        tag_with_id: str = None,
        rtree_search_box: float = None
) -> Dict[str, List[Dict[str, Any]]]:

    # prepare parameters
    db_file_path = db_file_path if db_file_path is not None else database.DEFAULT_DB_FILE_PATH
    mo_params = {}
    if tag_with_id:
        mo_params['tag_with_id'] = tag_with_id
    if rtree_search_box:
        mo_params['rtree_search_box'] = rtree_search_box

    # start process
    # download nodes from Overpass API
    db = database.DB(db_file_path)
    download_and_store_data(overpass_query_or_path, db)
    nodes = db.select_nodes()

    # match data already in OSM to data we want ot import to determine what is missing in OSM
    matches = match_objects(data, nodes, **mo_params, store_in_db=db_file_path)
    osm_id_to_object_id, object_id_to_osm_id = db.select_node_id_mappings()
    object_ids_in_osm = set(object_id_to_osm_id.keys())

    # filter out objects that are already in OSM
    objects_to_import = [x for x in data if x.get('id') not in object_ids_in_osm]
    objects_already_in_osm = [x for x in data if x.get('id') in object_ids_in_osm]

    # create nodes in osm
    prepared_objects_to_import = [
        {'lat': x['lat'], 'lon': x['lon'], 'tag': x.get('tag', {})} for x in objects_to_import
    ]
    changeset_tags = {}
    new_nodes = osm_api.create_nodes(prepared_objects_to_import, changeset_tags)
    db.upsert_nodes(new_nodes)

    results = {
        'objects_already_in_osm': objects_already_in_osm,
        'objects_imported_to_osm': objects_to_import
    }
    return results
