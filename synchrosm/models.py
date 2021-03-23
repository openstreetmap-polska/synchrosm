"""Classes"""

import json
from dataclasses import dataclass, field
from typing import Dict, Any, Union, Tuple, List


@dataclass(frozen=True, order=False)
class Node:
    """Class representing OSM Node."""

    id: int
    version: int
    latitude: float
    longitude: float
    tags: Dict[str, str]
    metadata: Dict[str, Union[str, int]]

    def as_api_dict(self) -> Dict[str, Any]:
        """Returns dictionary with a format matching the one expected by OsmApi package when updating nodes."""
        return {
            'id': self.id,
            'version': self.version,
            'lat': self.latitude,
            'lon': self.longitude,
            'tag': self.tags,
        }

    def as_tuple(self) -> Tuple[Union[str, int, float], ...]:
        """Returns tuple of values. Useful when importing data into a database.
        Tags and metadata are serialized to JSON string."""

        return (
            self.id,
            self.version,
            self.latitude,
            self.longitude,
            json.dumps(self.tags),
            json.dumps(self.metadata)
        )

    def __repr__(self):
        if self.latitude is not None and self.longitude is not None:
            return f'<OSM Node: {self.id} version {self.version} at {self.latitude:.5f} {self.longitude:.5f} >'
        else:
            return f'<OSM Node: {self.id} version {self.version} DELETED >'

    def __hash__(self):
        return hash('node' + str(self.id) + 'v' + str(self.version))

    def __eq__(self, other):
        return 'node' + str(self.id) + 'v' + str(self.version) == other


@dataclass
class NodeComparison:
    """Class holding results of comparison of two Node instances. Holds older version of the Node and newer version."""

    old: Node
    new: Node

    # def compare(self) -> :


@dataclass
class ComparisonResults:
    """Class holding results of comparison of Nodes stored in database to OSM API.
    Holds a list of Nodes that have the same version in OSM and a list of Nodes that have newer version in OSM."""

    unchanged: List[Node] = field(default_factory=list)
    new_version_in_osm: List[NodeComparison] = field(default_factory=list)


@dataclass
class NodeIdMapping:
    """Class holding mapping of OSM id to external object (one that we want to import to OSM) id."""

    osm_id: int
    object_id: str

    def as_tuple(self) -> Tuple[int, str]:
        """Returns values as tuple. Useful when importing to database."""

        return self.osm_id, self.object_id
