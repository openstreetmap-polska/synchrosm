insert into osm_base_timestamp (id, ts) values (?, ?)
ON CONFLICT (id)
DO UPDATE SET ts = excluded.ts
