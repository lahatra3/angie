ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET citus.shard_replication_factor = 2;
ALTER SYSTEM SET citus.shard_count = 4;
