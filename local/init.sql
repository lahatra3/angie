SELECT * FROM citus_add_node('172.17.0.1', 5433);
SELECT * FROM citus_add_node('172.17.0.1', 5434);
SELECT * FROM citus_add_node('172.17.0.1', 5435);
SELECT * FROM citus_add_node('172.17.0.1', 5436);

SELECT * FROM citus_get_active_worker_nodes();


CREATE SCHEMA app;

DROP TABLE app.users;

SET citus.shard_replication_factor = 2;

SHOW citus.shard_replication_factor;

-- 1. On crée la table parente (vide) avec la règle de partitionnement Postgres
CREATE TABLE IF NOT EXISTS app.users (
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    msisdn VARCHAR(50),
    daytime DATE
) PARTITION BY RANGE (daytime);

SET citus.shard_count = 4;
-- 2. IMPORTANT : On distribue la table parente IMMEDIATEMENT (lorsqu'elle est encore vide)
SELECT create_distributed_table('app.users', 'msisdn');

-- 3. Enfin, on génère les partitions quotidiennes avec la fonction de Citus
SELECT create_time_partitions(
  table_name         := 'app.users',
  partition_interval := '1 day',
  start_from         := '2026-07-01',
  end_at             := '2026-08-01'
);


SELECT * from pg_dist_node;

SELECT * from pg_dist_shard_placement;

SELECT * from pg_dist_placement;

SELECT 
    table_name,
    shardid,
    shard_name 
FROM citus_shards
where table_name::text ~ 'app.users_p[a-z0-9]{1,}'
and citus_table_type = 'distributed';


SELECT logicalrelid, partkey, partmethod FROM pg_dist_partition;

SELECT shardid, logicalrelid, shardminvalue, shardmaxvalue FROM pg_dist_shard;

SELECT shardid, logicalrelid, shardminvalue, shardmaxvalue 
FROM pg_dist_shard 

SELECT * FROM pg_dist_shard WHERE logicalrelid = 'app.users'::regclass;

SELECT logicalrelid, count(*) as nombre_de_shards
FROM pg_dist_shard 
WHERE logicalrelid::text LIKE 'app.users%'
GROUP BY logicalrelid;

show citus.shard_replication_factor;

SELECT
    -- DISTINCT ON (shard_name)
    table_name,
    shardid,
    shard_name,
    nodename
FROM citus_shards
where table_name::text ~ 'app.users_p[a-z0-9]{1,}'
and citus_table_type = 'distributed';


-- =========================== ********** ===========================
-- =========================== ********** ===========================
-- =========================== ********** ===========================
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
SHOW wal_level;
SHOW max_replication_slots;

SET citus.shard_replication_factor = 2;
SHOW citus.shard_replication_factor;

DROP TABLE IF EXISTS public.users;
CREATE TABLE IF NOT EXISTS public.users(
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    username VARCHAR(255),
    rank INT
);

SET citus.shard_count = 4;
SHOW citus.shard_count;
SELECT create_distributed_table('public.users', 'rank');

SELECT
    concat(table_name, '_', shardid) as target_table,
    shard_name,
    nodename,
    nodeport
FROM citus_shards
where citus_table_type = 'distributed'

-- users_102008, 172.17.0.1:5433
-- users_102009, 172.17.0.1:5434
-- users_102010, 172.17.0.1:5435
-- users_102011, 172.17.0.1:5436



SELECT pg_drop_replication_slot('cdc_slot');
SELECT pg_terminate_backend(active_pid) 
FROM pg_replication_slots 
WHERE slot_name = 'cdc_slot' AND active = true;

DROP PUBLICATION IF EXISTS cdc_pub;
CREATE PUBLICATION cdc_pub FOR TABLE public.users;

INSERT INTO public.users (firstname, lastname, username, rank)
VALUES
('Kristy', 'Jesoa', 'mesia', 1),
('Lahatra Anjara', 'RAVELONARIVO', 'lahatra3', 31);

SET citus.enable_change_data_capture = ON;
SHOW citus.enable_change_data_capture;

SELECT * FROM citus_add_node('172.17.0.1', 5433);
SELECT * FROM citus_add_node('172.17.0.1', 5434);
SELECT * FROM citus_add_node('172.17.0.1', 5435);
SELECT * FROM citus_add_node('172.17.0.1', 5436);


SELECT * FROM citus_get_active_worker_nodes();

ALTER SYSTEM SET citus.enable_ddl_propagation = off;

SHOW citus.enable_ddl_propagation;
SET citus.enable_ddl_propagation = on;

truncate table public.users;

-- =========================== ********** ===========================
-- =========================== ********** ===========================
-- =========================== ********** ===========================

-- '172.17.0.1:5433'
SHOW wal_level;
SHOW max_replication_slots;

select * from public.users_102008;

DROP PUBLICATION IF EXISTS cdc_pub;
CREATE PUBLICATION cdc_pub FOR TABLE public.users_102008;

SET citus.enable_change_data_capture = ON;
SHOW citus.enable_change_data_capture;

SET citus.enable_ddl_propagation = off;

SHOW citus.enable_ddl_propagation;