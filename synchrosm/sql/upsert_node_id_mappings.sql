INSERT INTO node_id_mappings VALUES (?, ?)
ON CONFLICT (osm_id, object_id)
DO UPDATE SET (osm_id, object_id) = (excluded.osm_id, excluded.object_id)
