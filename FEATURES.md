# mylite Feature Support

Auto-generated from `executor/features.go`. Do not edit manually.

**Summary:** 90 features — ✅ 41 supported, ⚠️ 30 partial, ❌ 19 unsupported

## Categories

- [Storage Engine](#storage-engine) (9)
- [DML](#dml) (8)
- [DDL](#ddl) (17)
- [SQL Statement](#sql-statement) (12)
- [Transaction](#transaction) (5)
- [Stored Program](#stored-program) (10)
- [Partitioning](#partitioning) (1)
- [Index](#index) (3)
- [Auth](#auth) (7)
- [Prepared Statement](#prepared-statement) (2)
- [Metadata](#metadata) (5)
- [Replication](#replication) (4)
- [CTE](#cte) (2)
- [Function](#function) (1)
- [Charset](#charset) (4)

## Storage Engine

| Feature | Status | Description |
|---|---|---|
| ENGINE=InnoDB | ✅ Supported | Default and only storage engine |
| ENGINE=MyISAM | ❌ Unsupported | Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE to MyISAM returns Error 50001 |
| ENGINE=MEMORY / HEAP | ❌ Unsupported | Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001 |
| ENGINE=MERGE / MRG_MYISAM | ❌ Unsupported | Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001 |
| ENGINE=CSV | ❌ Unsupported | Accepted but requires all columns to be NOT NULL; ALTER TABLE returns Error 50001 |
| ENGINE=ARCHIVE | ❌ Unsupported | Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001 |
| ENGINE=BLACKHOLE | ❌ Unsupported | Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001 |
| ENGINE=FEDERATED | ❌ Unsupported | Returns ER_UNKNOWN_STORAGE_ENGINE (1286) |
| ENGINE=NDB / NDBCLUSTER | ❌ Unsupported | Accepted in ALTER TABLE but ignored (substituted with InnoDB) |

## DML

| Feature | Status | Description |
|---|---|---|
| SELECT | ✅ Supported | Including subqueries, JOINs, UNION, GROUP BY, ORDER BY, LIMIT |
| INSERT | ✅ Supported | Including INSERT ... SELECT and ON DUPLICATE KEY UPDATE |
| UPDATE | ✅ Supported | Including multi-table UPDATE |
| DELETE | ✅ Supported | Including multi-table DELETE |
| REPLACE | ✅ Supported |  |
| SELECT INTO OUTFILE | ✅ Supported | Writes result set to a file on the server |
| LOAD DATA INFILE | ⚠️ Partial | Basic LOAD DATA works; some options (e.g. SET clause) may be limited |
| LOAD XML INFILE | ❌ Unsupported | Returns Error 50001 |

## DDL

| Feature | Status | Description |
|---|---|---|
| CREATE TABLE | ✅ Supported | Including LIKE, CREATE TABLE AS SELECT, generated columns |
| ALTER TABLE | ✅ Supported | Including ADD/DROP/MODIFY COLUMN, ADD/DROP INDEX, RENAME, ORDER BY |
| DROP TABLE | ✅ Supported |  |
| CREATE DATABASE | ✅ Supported |  |
| ALTER DATABASE | ✅ Supported |  |
| DROP DATABASE | ✅ Supported |  |
| CREATE INDEX | ✅ Supported |  |
| DROP INDEX | ✅ Supported |  |
| CREATE VIEW | ⚠️ Partial | Basic views supported; WITH CHECK OPTION supported; complex views may fail |
| DROP VIEW | ✅ Supported |  |
| CREATE TEMPORARY TABLE | ✅ Supported |  |
| RENAME TABLE | ✅ Supported |  |
| TRUNCATE TABLE | ✅ Supported |  |
| IMPORT TABLE | ❌ Unsupported | Returns Error 50001 |
| ALTER TABLE ... DISCARD/IMPORT TABLESPACE | ⚠️ Partial | Accepted as no-op (InnoDB transportable tablespace not implemented) |
| ALTER TABLE ... ORDER BY | ✅ Supported |  |
| CREATE UNDO TABLESPACE | ⚠️ Partial | Accepted as no-op; MyISAM engine returns ER_ILLEGAL_HA_CREATE_OPTION |

## SQL Statement

| Feature | Status | Description |
|---|---|---|
| HANDLER | ❌ Unsupported | HANDLER OPEN/READ/CLOSE returns Error 50001 |
| INSTALL COMPONENT | ❌ Unsupported | Returns Error 50001 |
| UNINSTALL COMPONENT | ❌ Unsupported | Returns Error 50001 |
| INSTALL PLUGIN | ⚠️ Partial | Plugin paths with '/' return ER_UDF_NO_PATHS; otherwise accepted as no-op |
| UNINSTALL PLUGIN | ⚠️ Partial | Accepted as no-op |
| XA Transactions | ❌ Unsupported | XA statements accepted as no-op |
| BINLOG | ❌ Unsupported | Accepted as no-op |
| CHECK TABLE | ⚠️ Partial | Accepted; returns OK status |
| REPAIR TABLE | ⚠️ Partial | Accepted as no-op |
| OPTIMIZE TABLE | ⚠️ Partial | Accepted as no-op |
| CHECKSUM TABLE | ⚠️ Partial | Accepted as no-op |
| FLUSH | ⚠️ Partial | FLUSH PRIVILEGES reloads grant store; other FLUSH variants are no-ops |

## Transaction

| Feature | Status | Description |
|---|---|---|
| BEGIN / COMMIT / ROLLBACK | ✅ Supported |  |
| SAVEPOINT | ✅ Supported | SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT |
| SELECT FOR UPDATE | ✅ Supported | Row-level locking via lock manager |
| LOCK TABLES | ⚠️ Partial | Basic locking accepted; performance_schema tables return privilege error |
| UNLOCK TABLES | ✅ Supported |  |

## Stored Program

| Feature | Status | Description |
|---|---|---|
| CREATE PROCEDURE | ✅ Supported |  |
| DROP PROCEDURE | ✅ Supported |  |
| ALTER PROCEDURE | ⚠️ Partial | Accepted as no-op |
| CREATE FUNCTION | ✅ Supported | Including user-defined functions |
| DROP FUNCTION | ✅ Supported |  |
| ALTER FUNCTION | ⚠️ Partial | Accepted as no-op |
| CREATE TRIGGER | ⚠️ Partial | Basic triggers supported; complex control flow may fail |
| DROP TRIGGER | ✅ Supported |  |
| CREATE EVENT | ⚠️ Partial | Event creation accepted as no-op; scheduler not implemented; MICROSECOND interval returns Error 1235 |
| DROP EVENT | ⚠️ Partial | Accepted as no-op |

## Partitioning

| Feature | Status | Description |
|---|---|---|
| PARTITION BY | ⚠️ Partial | CREATE TABLE accepts PARTITION BY but stores as single partition (partitioning not enforced) |

## Index

| Feature | Status | Description |
|---|---|---|
| B-Tree Index | ✅ Supported | Default index type for InnoDB |
| FULLTEXT Index | ⚠️ Partial | Index creation accepted; MATCH ... AGAINST has limited support |
| Spatial Index | ⚠️ Partial | Some GIS functions work; spatial indexing not fully implemented |

## Auth

| Feature | Status | Description |
|---|---|---|
| CREATE USER | ✅ Supported |  |
| DROP USER | ✅ Supported |  |
| ALTER USER | ⚠️ Partial | Basic ALTER USER accepted; some options are no-ops |
| GRANT / REVOKE | ✅ Supported |  |
| Roles | ⚠️ Partial | CREATE ROLE / DROP ROLE accepted; role assignment supported; some edge cases differ |
| SHOW GRANTS | ✅ Supported |  |
| SET PASSWORD | ✅ Supported |  |

## Prepared Statement

| Feature | Status | Description |
|---|---|---|
| PREPARE / EXECUTE | ✅ Supported | Including PREPARE from user variables |
| DEALLOCATE PREPARE | ✅ Supported |  |

## Metadata

| Feature | Status | Description |
|---|---|---|
| INFORMATION_SCHEMA | ⚠️ Partial | Common tables supported; some columns may differ from MySQL |
| PERFORMANCE_SCHEMA | ⚠️ Partial | Limited subset of tables; setup_actors and setup_objects supported |
| sys schema | ⚠️ Partial | Selected sys views available |
| SHOW commands | ✅ Supported | SHOW TABLES, DATABASES, CREATE TABLE, COLUMNS, STATUS, GRANTS, etc. |
| EXPLAIN | ⚠️ Partial | Basic EXPLAIN supported; output format may differ from MySQL |

## Replication

| Feature | Status | Description |
|---|---|---|
| Binary Log | ❌ Unsupported | No replication support; BINLOG statements are no-ops |
| GTID | ❌ Unsupported | GTID variables exist but replication is not implemented |
| CHANGE REPLICATION SOURCE / CHANGE MASTER | ❌ Unsupported | Accepted as no-op |
| START / STOP REPLICA | ❌ Unsupported | Accepted as no-op |

## CTE

| Feature | Status | Description |
|---|---|---|
| WITH (non-recursive CTE) | ✅ Supported |  |
| WITH RECURSIVE | ⚠️ Partial | Basic recursive CTEs work; cte_max_recursion_depth enforced; some validation errors may differ |

## Function

| Feature | Status | Description |
|---|---|---|
| Window Functions | ⚠️ Partial | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc. supported; some edge cases may differ |

## Charset

| Feature | Status | Description |
|---|---|---|
| utf8mb4 | ✅ Supported | Default charset; full Unicode support |
| utf8 (utf8mb3) | ✅ Supported | Accepted; internally treated as utf8mb4 |
| Non-UTF8 charsets | ⚠️ Partial | Charset names accepted; actual encoding conversion is limited (cp1250, cp1251, cp1256, cp1257, koi8r, etc.) |
| Collations | ⚠️ Partial | Common collations recognized; case-insensitive comparison implemented; binary collations supported |

