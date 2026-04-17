package executor

// FeatureStatus represents the support status of a MySQL feature.
type FeatureStatus string

const (
	Supported   FeatureStatus = "supported"
	Unsupported FeatureStatus = "unsupported"
	Partial     FeatureStatus = "partial"
)

// Feature describes a MySQL feature and its support level in mylite.
type Feature struct {
	Name        string
	Category    string        // "Storage Engine", "DML", "DDL", "Transaction", etc.
	Status      FeatureStatus
	Description string // one-line description
}

// Features is the canonical registry of MySQL feature support in mylite.
var Features = []Feature{
	// Storage Engines
	// CREATE TABLE accepts MyISAM/MEMORY/etc without NO_ENGINE_SUBSTITUTION; treated as InnoDB.
	// ALTER TABLE to MyISAM/MEMORY/HEAP/MERGE/BLACKHOLE/ARCHIVE returns Error 50001.
	{Name: "ENGINE=InnoDB", Category: "Storage Engine", Status: Supported, Description: "Default and only storage engine"},
	{Name: "ENGINE=MyISAM", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE to MyISAM returns Error 50001"},
	{Name: "ENGINE=MEMORY / HEAP", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001"},
	{Name: "ENGINE=MERGE / MRG_MYISAM", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001"},
	{Name: "ENGINE=CSV", Category: "Storage Engine", Status: Unsupported, Description: "Accepted but requires all columns to be NOT NULL; ALTER TABLE returns Error 50001"},
	{Name: "ENGINE=ARCHIVE", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001"},
	{Name: "ENGINE=BLACKHOLE", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in CREATE TABLE (treated as InnoDB); ALTER TABLE returns Error 50001"},
	{Name: "ENGINE=FEDERATED", Category: "Storage Engine", Status: Unsupported, Description: "Returns ER_UNKNOWN_STORAGE_ENGINE (1286)"},
	{Name: "ENGINE=NDB / NDBCLUSTER", Category: "Storage Engine", Status: Unsupported, Description: "Accepted in ALTER TABLE but ignored (substituted with InnoDB)"},

	// DML
	{Name: "SELECT", Category: "DML", Status: Supported, Description: "Including subqueries, JOINs, UNION, GROUP BY, ORDER BY, LIMIT"},
	{Name: "INSERT", Category: "DML", Status: Supported, Description: "Including INSERT ... SELECT and ON DUPLICATE KEY UPDATE"},
	{Name: "UPDATE", Category: "DML", Status: Supported, Description: "Including multi-table UPDATE"},
	{Name: "DELETE", Category: "DML", Status: Supported, Description: "Including multi-table DELETE"},
	{Name: "REPLACE", Category: "DML", Status: Supported, Description: ""},
	{Name: "SELECT INTO OUTFILE", Category: "DML", Status: Supported, Description: "Writes result set to a file on the server"},
	{Name: "LOAD DATA INFILE", Category: "DML", Status: Partial, Description: "Basic LOAD DATA works; some options (e.g. SET clause) may be limited"},
	{Name: "LOAD XML INFILE", Category: "DML", Status: Unsupported, Description: "Returns Error 50001"},

	// DDL
	{Name: "CREATE TABLE", Category: "DDL", Status: Supported, Description: "Including LIKE, CREATE TABLE AS SELECT, generated columns"},
	{Name: "ALTER TABLE", Category: "DDL", Status: Supported, Description: "Including ADD/DROP/MODIFY COLUMN, ADD/DROP INDEX, RENAME, ORDER BY"},
	{Name: "DROP TABLE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "CREATE DATABASE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "ALTER DATABASE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "DROP DATABASE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "CREATE INDEX", Category: "DDL", Status: Supported, Description: ""},
	{Name: "DROP INDEX", Category: "DDL", Status: Supported, Description: ""},
	{Name: "CREATE VIEW", Category: "DDL", Status: Partial, Description: "Basic views supported; WITH CHECK OPTION supported; complex views may fail"},
	{Name: "DROP VIEW", Category: "DDL", Status: Supported, Description: ""},
	{Name: "CREATE TEMPORARY TABLE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "RENAME TABLE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "TRUNCATE TABLE", Category: "DDL", Status: Supported, Description: ""},
	{Name: "IMPORT TABLE", Category: "DDL", Status: Unsupported, Description: "Returns Error 50001"},
	{Name: "ALTER TABLE ... DISCARD/IMPORT TABLESPACE", Category: "DDL", Status: Partial, Description: "Accepted as no-op (InnoDB transportable tablespace not implemented)"},
	{Name: "ALTER TABLE ... ORDER BY", Category: "DDL", Status: Supported, Description: ""},
	{Name: "CREATE UNDO TABLESPACE", Category: "DDL", Status: Partial, Description: "Accepted as no-op; MyISAM engine returns ER_ILLEGAL_HA_CREATE_OPTION"},

	// SQL Statements
	{Name: "HANDLER", Category: "SQL Statement", Status: Unsupported, Description: "HANDLER OPEN/READ/CLOSE returns Error 50001"},
	{Name: "INSTALL COMPONENT", Category: "SQL Statement", Status: Unsupported, Description: "Returns Error 50001"},
	{Name: "UNINSTALL COMPONENT", Category: "SQL Statement", Status: Unsupported, Description: "Returns Error 50001"},
	{Name: "INSTALL PLUGIN", Category: "SQL Statement", Status: Partial, Description: "Plugin paths with '/' return ER_UDF_NO_PATHS; otherwise accepted as no-op"},
	{Name: "UNINSTALL PLUGIN", Category: "SQL Statement", Status: Partial, Description: "Accepted as no-op"},
	{Name: "XA Transactions", Category: "SQL Statement", Status: Unsupported, Description: "XA statements accepted as no-op"},
	{Name: "BINLOG", Category: "SQL Statement", Status: Unsupported, Description: "Accepted as no-op"},
	{Name: "CHECK TABLE", Category: "SQL Statement", Status: Partial, Description: "Accepted; returns OK status"},
	{Name: "REPAIR TABLE", Category: "SQL Statement", Status: Partial, Description: "Accepted as no-op"},
	{Name: "OPTIMIZE TABLE", Category: "SQL Statement", Status: Partial, Description: "Accepted as no-op"},
	{Name: "CHECKSUM TABLE", Category: "SQL Statement", Status: Partial, Description: "Accepted as no-op"},
	{Name: "FLUSH", Category: "SQL Statement", Status: Partial, Description: "FLUSH PRIVILEGES reloads grant store; other FLUSH variants are no-ops"},

	// Transactions
	{Name: "BEGIN / COMMIT / ROLLBACK", Category: "Transaction", Status: Supported, Description: ""},
	{Name: "SAVEPOINT", Category: "Transaction", Status: Supported, Description: "SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT"},
	{Name: "SELECT FOR UPDATE", Category: "Transaction", Status: Supported, Description: "Row-level locking via lock manager"},
	{Name: "LOCK TABLES", Category: "Transaction", Status: Partial, Description: "Basic locking accepted; performance_schema tables return privilege error"},
	{Name: "UNLOCK TABLES", Category: "Transaction", Status: Supported, Description: ""},

	// Stored Programs
	{Name: "CREATE PROCEDURE", Category: "Stored Program", Status: Supported, Description: ""},
	{Name: "DROP PROCEDURE", Category: "Stored Program", Status: Supported, Description: ""},
	{Name: "ALTER PROCEDURE", Category: "Stored Program", Status: Partial, Description: "Accepted as no-op"},
	{Name: "CREATE FUNCTION", Category: "Stored Program", Status: Supported, Description: "Including user-defined functions"},
	{Name: "DROP FUNCTION", Category: "Stored Program", Status: Supported, Description: ""},
	{Name: "ALTER FUNCTION", Category: "Stored Program", Status: Partial, Description: "Accepted as no-op"},
	{Name: "CREATE TRIGGER", Category: "Stored Program", Status: Partial, Description: "Basic triggers supported; complex control flow may fail"},
	{Name: "DROP TRIGGER", Category: "Stored Program", Status: Supported, Description: ""},
	{Name: "CREATE EVENT", Category: "Stored Program", Status: Partial, Description: "Event creation accepted as no-op; scheduler not implemented; MICROSECOND interval returns Error 1235"},
	{Name: "DROP EVENT", Category: "Stored Program", Status: Partial, Description: "Accepted as no-op"},

	// Partitioning
	{Name: "PARTITION BY", Category: "Partitioning", Status: Partial, Description: "CREATE TABLE accepts PARTITION BY but stores as single partition (partitioning not enforced)"},

	// Indexes
	{Name: "B-Tree Index", Category: "Index", Status: Supported, Description: "Default index type for InnoDB"},
	{Name: "FULLTEXT Index", Category: "Index", Status: Partial, Description: "Index creation accepted; MATCH ... AGAINST has limited support"},
	{Name: "Spatial Index", Category: "Index", Status: Partial, Description: "Some GIS functions work; spatial indexing not fully implemented"},

	// Authentication & Privileges
	{Name: "CREATE USER", Category: "Auth", Status: Supported, Description: ""},
	{Name: "DROP USER", Category: "Auth", Status: Supported, Description: ""},
	{Name: "ALTER USER", Category: "Auth", Status: Partial, Description: "Basic ALTER USER accepted; some options are no-ops"},
	{Name: "GRANT / REVOKE", Category: "Auth", Status: Supported, Description: ""},
	{Name: "Roles", Category: "Auth", Status: Partial, Description: "CREATE ROLE / DROP ROLE accepted; role assignment supported; some edge cases differ"},
	{Name: "SHOW GRANTS", Category: "Auth", Status: Supported, Description: ""},
	{Name: "SET PASSWORD", Category: "Auth", Status: Supported, Description: ""},

	// Prepared Statements
	{Name: "PREPARE / EXECUTE", Category: "Prepared Statement", Status: Supported, Description: "Including PREPARE from user variables"},
	{Name: "DEALLOCATE PREPARE", Category: "Prepared Statement", Status: Supported, Description: ""},

	// Metadata / Information Schema
	{Name: "INFORMATION_SCHEMA", Category: "Metadata", Status: Partial, Description: "Common tables supported; some columns may differ from MySQL"},
	{Name: "PERFORMANCE_SCHEMA", Category: "Metadata", Status: Partial, Description: "Limited subset of tables; setup_actors and setup_objects supported"},
	{Name: "sys schema", Category: "Metadata", Status: Partial, Description: "Selected sys views available"},
	{Name: "SHOW commands", Category: "Metadata", Status: Supported, Description: "SHOW TABLES, DATABASES, CREATE TABLE, COLUMNS, STATUS, GRANTS, etc."},
	{Name: "EXPLAIN", Category: "Metadata", Status: Partial, Description: "Basic EXPLAIN supported; output format may differ from MySQL"},

	// Replication
	{Name: "Binary Log", Category: "Replication", Status: Unsupported, Description: "No replication support; BINLOG statements are no-ops"},
	{Name: "GTID", Category: "Replication", Status: Unsupported, Description: "GTID variables exist but replication is not implemented"},
	{Name: "CHANGE REPLICATION SOURCE / CHANGE MASTER", Category: "Replication", Status: Unsupported, Description: "Accepted as no-op"},
	{Name: "START / STOP REPLICA", Category: "Replication", Status: Unsupported, Description: "Accepted as no-op"},

	// CTEs
	{Name: "WITH (non-recursive CTE)", Category: "CTE", Status: Supported, Description: ""},
	{Name: "WITH RECURSIVE", Category: "CTE", Status: Partial, Description: "Basic recursive CTEs work; cte_max_recursion_depth enforced; some validation errors may differ"},

	// Window Functions
	{Name: "Window Functions", Category: "Function", Status: Partial, Description: "ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc. supported; some edge cases may differ"},

	// Character Sets & Collations
	{Name: "utf8mb4", Category: "Charset", Status: Supported, Description: "Default charset; full Unicode support"},
	{Name: "utf8 (utf8mb3)", Category: "Charset", Status: Supported, Description: "Accepted; internally treated as utf8mb4"},
	{Name: "Non-UTF8 charsets", Category: "Charset", Status: Partial, Description: "Charset names accepted; actual encoding conversion is limited (cp1250, cp1251, cp1256, cp1257, koi8r, etc.)"},
	{Name: "Collations", Category: "Charset", Status: Partial, Description: "Common collations recognized; case-insensitive comparison implemented; binary collations supported"},
}
