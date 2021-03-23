"""Functions for interacting with sqlite database that allows to persist data between runs."""

import json
import logging
from datetime import datetime
from os import path
from typing import Tuple, Union, Iterable, List, Dict
import sqlite3

from synchrosm.models import Node, NodeIdMapping

# Create a custom logger
logger = logging.getLogger('DB')

SQL_DIR: str = path.join(path.dirname(path.abspath(__file__)), 'sql')
SQLITE_COMPATIBLE_TYPES = Union[str, int, float]
TUPLE_OF_SQLITE_COMPATIBLE_TYPES = Tuple[SQLITE_COMPATIBLE_TYPES, ...]
DEFAULT_DB_FILE_PATH = 'db.sqlite'


class DB:
    """Class for interacting with sqlite database that will persist data downloaded from OSM."""

    def __init__(self, db_file_path: str = DEFAULT_DB_FILE_PATH):

        self.db_file_path = db_file_path
        self.create_tables()

    def create_tables(self) -> None:
        """Create tables in the database if they don't exist. Automatically called when initialising the class."""

        with open(path.join(SQL_DIR, 'create_tables.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            db.executescript(query)
            db.commit()
            logger.info('Created tables.')

    def clear_tables(self) -> None:
        """Remove all data from tables."""

        with open(path.join(SQL_DIR, 'clear_tables.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            db.executescript(query)
            db.commit()
            logger.info('Truncated tables.')

    def set_osm_base_timestamp(self, timestamp: datetime) -> None:
        """Set value of OSM base timestamp returned by Overpass API.
        This timestamp marks state of OSM database when querying Overpass API."""

        with open(path.join(SQL_DIR, 'upsert_osm_base_timestamp.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            cur.execute(query, (1, timestamp.isoformat()))
            db.commit()
            logger.info('Updated database. Rows: 1')

    def get_osm_base_timestamp(self) -> datetime:
        """Get value of OSM base timestamp returned by Overpass API.
        This timestamp marks state of OSM database when querying Overpass API."""

        with open(path.join(SQL_DIR, 'select_osm_base_timestamp.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            cur.execute(query)
            ts = cur.fetchone()
        logger.info('Selected from database. Rows: 1')
        return datetime.fromisoformat(ts)

    def upsert_nodes(self, list_of_nodes: Iterable[Node]) -> None:
        """Insert or update Nodes stored in the database."""

        with open(path.join(SQL_DIR, 'upsert_nodes.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            params = [node.as_tuple() for node in list_of_nodes]
            cur.executemany(query, params)
            db.commit()
            logger.info(f'Updated database. Rows: {len(params)}')

    def select_nodes(self, limit: int = None) -> List[Node]:
        """Select Nodes stored in the database. You may provide parameter that limits number of returned rows."""

        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            if limit:
                cur.execute('SELECT * FROM nodes LIMIT ?', (limit,))
            else:
                cur.execute('SELECT * FROM nodes')
            tuples = cur.fetchall()
            results = [Node(n[0], n[1], n[2], n[3], json.loads(n[4]), json.loads(n[5])) for n in tuples]
        logger.info(f'Selected from database. Rows: {len(results)}')
        return results

    def delete_nodes(self, list_of_ids: List[int]) -> None:
        """Delete nodes from the database."""

        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            cur.executemany('DELETE FROM nodes WHERE id = ?', [(id,) for id in list_of_ids])
            db.commit()
            logger.info(f'Updated database. Rows: {len(list_of_ids)}')

    def upsert_node_id_mappings(self, list_of_mappings: Iterable[NodeIdMapping]) -> None:
        """Insert or update mappings between OSM id and external object id stored in the database."""

        with open(path.join(SQL_DIR, 'upsert_node_id_mappings.sql'), 'r', encoding='utf-8') as f:
            query = f.read()
        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            params = [m.as_tuple() for m in list_of_mappings]
            cur.executemany(query, params)
            db.commit()
            logger.info(f'Updated database. Rows: {len(params)}')

    def select_node_id_mappings(self) -> Tuple[Dict[int, str], Dict[str, int]]:
        """Select mappings between OSM id and external object id stored in the database.
        Returns tuple of two dictionaries: once that has OSM id as key and one that has object id as dictionary key."""

        with sqlite3.connect(self.db_file_path) as db:
            cur = db.cursor()
            cur.execute('SELECT osm_id, object_id FROM node_id_mappings')
            values = cur.fetchall()
            logger.info(f'Selected from database. Rows: {len(values)}')

            osm_id_to_object_id = {row[0]: row[1] for row in values}
            object_id_to_osm_id = {row[1]: row[0] for row in values}

            return osm_id_to_object_id, object_id_to_osm_id
