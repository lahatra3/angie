ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET citus.enable_ddl_propagation = OFF; -- disable DDL verification
