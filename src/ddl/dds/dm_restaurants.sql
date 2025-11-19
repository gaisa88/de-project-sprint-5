CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name TEXT NOT NULL,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL,
    CONSTRAINT uq_dm_restaurants UNIQUE (restaurant_id, active_from)
);
CREATE INDEX IF NOT EXISTS idx_dm_restaurants__restaurant_id_active_from
    ON dds.dm_restaurants (restaurant_id, active_from);