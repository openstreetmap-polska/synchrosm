INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (id)
DO UPDATE SET
(
    version,
    latitude,
    longitude,
    tags,
    metadata
) = (
    excluded.version,
    excluded.latitude,
    excluded.longitude,
    excluded.tags,
    excluded.metadata
)
