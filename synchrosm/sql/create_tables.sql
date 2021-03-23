create table if not exists osm_base_timestamp (id integer not null primary key, ts text not null);
create table if not exists nodes (
  id integer not null primary key,
  version integer not null,
  latitude real not null,
  longitude real not null,
  tags text,
  metadata text
);
create table if not exists node_id_mappings (
  osm_id integer not null,
  object_id text not null,
  primary key (osm_id, object_id)
);
