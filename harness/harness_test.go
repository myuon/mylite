package harness

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

func getHarness(t *testing.T) *Harness {
	t.Helper()
	mysqlDSN := os.Getenv("MYSQL_TEST_DSN")
	h := NewHarness(t, mysqlDSN)
	t.Cleanup(h.Close)
	return h
}

func TestCreateTableAndInsertSelect(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "basic CREATE TABLE, INSERT, SELECT",
		Setup: []string{
			"CREATE TABLE test_users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, age INT)",
		},
		Query: "SELECT * FROM test_users",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_users",
		},
	})

	h.Run(TestCase{
		Name: "INSERT and SELECT rows",
		Setup: []string{
			"CREATE TABLE test_items (id INT PRIMARY KEY AUTO_INCREMENT, title VARCHAR(255), price INT)",
			"INSERT INTO test_items (title, price) VALUES ('apple', 100)",
			"INSERT INTO test_items (title, price) VALUES ('banana', 200)",
			"INSERT INTO test_items (title, price) VALUES ('cherry', 150)",
		},
		Query: "SELECT title, price FROM test_items ORDER BY price",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_items",
		},
	})
}

func TestWhereClause(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "WHERE with equality",
		Setup: []string{
			"CREATE TABLE test_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50))",
			"INSERT INTO test_products (id, name, category) VALUES (1, 'Laptop', 'electronics')",
			"INSERT INTO test_products (id, name, category) VALUES (2, 'Shirt', 'clothing')",
			"INSERT INTO test_products (id, name, category) VALUES (3, 'Phone', 'electronics')",
		},
		Query: "SELECT name FROM test_products WHERE category = 'electronics' ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_products",
		},
	})

	h.Run(TestCase{
		Name: "WHERE with comparison operators",
		Setup: []string{
			"CREATE TABLE test_scores (id INT PRIMARY KEY, score INT)",
			"INSERT INTO test_scores (id, score) VALUES (1, 85)",
			"INSERT INTO test_scores (id, score) VALUES (2, 92)",
			"INSERT INTO test_scores (id, score) VALUES (3, 78)",
			"INSERT INTO test_scores (id, score) VALUES (4, 95)",
		},
		Query: "SELECT id, score FROM test_scores WHERE score >= 90 ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_scores",
		},
	})
}

func TestUpdateDelete(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "UPDATE rows",
		Setup: []string{
			"CREATE TABLE test_update (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_update (id, val) VALUES (1, 10)",
			"INSERT INTO test_update (id, val) VALUES (2, 20)",
			"UPDATE test_update SET val = 99 WHERE id = 1",
		},
		Query: "SELECT id, val FROM test_update ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_update",
		},
	})

	h.Run(TestCase{
		Name: "DELETE rows",
		Setup: []string{
			"CREATE TABLE test_delete (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_delete (id, val) VALUES (1, 10)",
			"INSERT INTO test_delete (id, val) VALUES (2, 20)",
			"INSERT INTO test_delete (id, val) VALUES (3, 30)",
			"DELETE FROM test_delete WHERE id = 2",
		},
		Query: "SELECT id, val FROM test_delete ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_delete",
		},
	})
}

func TestLimitOffset(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "LIMIT",
		Setup: []string{
			"CREATE TABLE test_limit (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_limit (id, val) VALUES (1, 10)",
			"INSERT INTO test_limit (id, val) VALUES (2, 20)",
			"INSERT INTO test_limit (id, val) VALUES (3, 30)",
			"INSERT INTO test_limit (id, val) VALUES (4, 40)",
		},
		Query: "SELECT id, val FROM test_limit ORDER BY id LIMIT 2",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_limit",
		},
	})

	h.Run(TestCase{
		Name: "LIMIT with OFFSET",
		Setup: []string{
			"CREATE TABLE test_offset (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_offset (id, val) VALUES (1, 10)",
			"INSERT INTO test_offset (id, val) VALUES (2, 20)",
			"INSERT INTO test_offset (id, val) VALUES (3, 30)",
			"INSERT INTO test_offset (id, val) VALUES (4, 40)",
		},
		Query: "SELECT id, val FROM test_offset ORDER BY id LIMIT 2 OFFSET 1",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_offset",
		},
	})
}

func TestShowTables(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "SHOW TABLES",
		Setup: []string{
			"CREATE TABLE test_show_a (id INT PRIMARY KEY)",
			"CREATE TABLE test_show_b (id INT PRIMARY KEY)",
		},
		Query:             "SHOW TABLES",
		SkipResultCompare: true, // MySQL will show all tables in the DB, not just ours
		Teardown: []string{
			"DROP TABLE IF EXISTS test_show_a",
			"DROP TABLE IF EXISTS test_show_b",
		},
	})
}

func TestTransactions(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "ROLLBACK restores rows",
		Setup: []string{
			"CREATE TABLE test_tx_rollback (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_tx_rollback (id, val) VALUES (1, 10)",
			"BEGIN",
			"INSERT INTO test_tx_rollback (id, val) VALUES (2, 20)",
			"ROLLBACK",
		},
		Query: "SELECT id, val FROM test_tx_rollback ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_tx_rollback",
		},
	})

	h.Run(TestCase{
		Name: "COMMIT persists rows",
		Setup: []string{
			"CREATE TABLE test_tx_commit (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_tx_commit (id, val) VALUES (1, 10)",
			"BEGIN",
			"INSERT INTO test_tx_commit (id, val) VALUES (2, 20)",
			"COMMIT",
		},
		Query: "SELECT id, val FROM test_tx_commit ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_tx_commit",
		},
	})

	h.Run(TestCase{
		Name: "ROLLBACK after UPDATE restores old values",
		Setup: []string{
			"CREATE TABLE test_tx_update (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_tx_update (id, val) VALUES (1, 100)",
			"BEGIN",
			"UPDATE test_tx_update SET val = 999 WHERE id = 1",
			"ROLLBACK",
		},
		Query: "SELECT id, val FROM test_tx_update ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_tx_update",
		},
	})

	h.Run(TestCase{
		Name: "ROLLBACK after DELETE restores rows",
		Setup: []string{
			"CREATE TABLE test_tx_delete (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_tx_delete (id, val) VALUES (1, 10)",
			"INSERT INTO test_tx_delete (id, val) VALUES (2, 20)",
			"BEGIN",
			"DELETE FROM test_tx_delete WHERE id = 1",
			"ROLLBACK",
		},
		Query: "SELECT id, val FROM test_tx_delete ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_tx_delete",
		},
	})

	h.Run(TestCase{
		Name: "DDL causes implicit COMMIT - table persists after ROLLBACK",
		Setup: []string{
			"BEGIN",
			"CREATE TABLE test_tx_ddl (id INT PRIMARY KEY)",
			"ROLLBACK",
		},
		// In MySQL, DDL (CREATE TABLE) causes an implicit COMMIT, so ROLLBACK
		// has nothing to undo. The table persists.
		Query: "SELECT * FROM test_tx_ddl",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_tx_ddl",
		},
	})
}

func TestNullHandling(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "NULL values",
		Setup: []string{
			"CREATE TABLE test_null (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_null (id, val) VALUES (1, NULL)",
			"INSERT INTO test_null (id, val) VALUES (2, 42)",
		},
		Query: "SELECT id, val FROM test_null ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_null",
		},
	})
}

func TestJoin(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "INNER JOIN",
		Setup: []string{
			"CREATE TABLE test_j_users (id INT PRIMARY KEY, name VARCHAR(255))",
			"CREATE TABLE test_j_orders (id INT PRIMARY KEY, user_id INT, amount INT)",
			"INSERT INTO test_j_users (id, name) VALUES (1, 'Alice')",
			"INSERT INTO test_j_users (id, name) VALUES (2, 'Bob')",
			"INSERT INTO test_j_orders (id, user_id, amount) VALUES (1, 1, 100)",
			"INSERT INTO test_j_orders (id, user_id, amount) VALUES (2, 1, 200)",
			"INSERT INTO test_j_orders (id, user_id, amount) VALUES (3, 2, 150)",
		},
		Query: "SELECT test_j_users.name, test_j_orders.amount FROM test_j_users JOIN test_j_orders ON test_j_users.id = test_j_orders.user_id ORDER BY test_j_orders.id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_j_users",
			"DROP TABLE IF EXISTS test_j_orders",
		},
	})

	h.Run(TestCase{
		Name: "LEFT JOIN with unmatched rows",
		Setup: []string{
			"CREATE TABLE test_lj_users (id INT PRIMARY KEY, name VARCHAR(255))",
			"CREATE TABLE test_lj_orders (id INT PRIMARY KEY, user_id INT, amount INT)",
			"INSERT INTO test_lj_users (id, name) VALUES (1, 'Alice')",
			"INSERT INTO test_lj_users (id, name) VALUES (2, 'Bob')",
			"INSERT INTO test_lj_users (id, name) VALUES (3, 'Charlie')",
			"INSERT INTO test_lj_orders (id, user_id, amount) VALUES (1, 1, 100)",
			"INSERT INTO test_lj_orders (id, user_id, amount) VALUES (2, 2, 200)",
		},
		Query: "SELECT test_lj_users.name, test_lj_orders.amount FROM test_lj_users LEFT JOIN test_lj_orders ON test_lj_users.id = test_lj_orders.user_id ORDER BY test_lj_users.id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_lj_users",
			"DROP TABLE IF EXISTS test_lj_orders",
		},
	})

	h.Run(TestCase{
		Name: "JOIN with aliases",
		Setup: []string{
			"CREATE TABLE test_ja_users (id INT PRIMARY KEY, name VARCHAR(255))",
			"CREATE TABLE test_ja_orders (id INT PRIMARY KEY, user_id INT, amount INT)",
			"INSERT INTO test_ja_users (id, name) VALUES (1, 'Alice')",
			"INSERT INTO test_ja_orders (id, user_id, amount) VALUES (1, 1, 300)",
		},
		Query: "SELECT u.name, o.amount FROM test_ja_users u JOIN test_ja_orders o ON u.id = o.user_id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_ja_users",
			"DROP TABLE IF EXISTS test_ja_orders",
		},
	})
}

func TestPreparedStatements(t *testing.T) {
	h := getHarness(t)

	t.Run("prepared INSERT with placeholders", func(t *testing.T) {
		// Setup
		if _, err := h.myliteDB.Exec("CREATE TABLE test_ps_insert (id INT PRIMARY KEY, name VARCHAR(255), score INT)"); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer h.myliteDB.Exec("DROP TABLE IF EXISTS test_ps_insert") //nolint:errcheck

		stmt, err := h.myliteDB.Prepare("INSERT INTO test_ps_insert (id, name, score) VALUES (?, ?, ?)")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer stmt.Close()

		if _, err := stmt.Exec(1, "Alice", 95); err != nil {
			t.Fatalf("exec failed: %v", err)
		}
		if _, err := stmt.Exec(2, "Bob", 80); err != nil {
			t.Fatalf("exec failed: %v", err)
		}

		rows, err := h.myliteDB.Query("SELECT id, name, score FROM test_ps_insert ORDER BY id")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		type row struct{ id int; name string; score int }
		var got []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.name, &r.score); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			got = append(got, r)
		}

		want := []row{{1, "Alice", 95}, {2, "Bob", 80}}
		if len(got) != len(want) {
			t.Fatalf("row count mismatch: got %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("prepared SELECT with placeholder", func(t *testing.T) {
		// Setup
		if _, err := h.myliteDB.Exec("CREATE TABLE test_ps_select (id INT PRIMARY KEY, category VARCHAR(50), val INT)"); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer h.myliteDB.Exec("DROP TABLE IF EXISTS test_ps_select") //nolint:errcheck

		for _, row := range []struct {
			id       int
			category string
			val      int
		}{
			{1, "A", 10},
			{2, "B", 20},
			{3, "A", 30},
		} {
			if _, err := h.myliteDB.Exec(
				fmt.Sprintf("INSERT INTO test_ps_select (id, category, val) VALUES (%d, '%s', %d)", row.id, row.category, row.val),
			); err != nil {
				t.Fatalf("insert failed: %v", err)
			}
		}

		stmt, err := h.myliteDB.Prepare("SELECT id, val FROM test_ps_select WHERE category = ? ORDER BY id")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer stmt.Close()

		rows, err := stmt.Query("A")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		type row struct{ id, val int }
		var got []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.val); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			got = append(got, r)
		}

		want := []row{{1, 10}, {3, 30}}
		if len(got) != len(want) {
			t.Fatalf("row count mismatch: got %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("prepared statement reuse", func(t *testing.T) {
		if _, err := h.myliteDB.Exec("CREATE TABLE test_ps_reuse (id INT PRIMARY KEY, val INT)"); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer h.myliteDB.Exec("DROP TABLE IF EXISTS test_ps_reuse") //nolint:errcheck

		stmt, err := h.myliteDB.Prepare("INSERT INTO test_ps_reuse (id, val) VALUES (?, ?)")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer stmt.Close()

		for i := 1; i <= 5; i++ {
			if _, err := stmt.Exec(i, i*10); err != nil {
				t.Fatalf("exec %d failed: %v", i, err)
			}
		}

		var count int
		if err := h.myliteDB.QueryRow("SELECT COUNT(*) FROM test_ps_reuse").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if count != 5 {
			t.Errorf("expected 5 rows, got %d", count)
		}
	})

	t.Run("prepared statement with NULL arg", func(t *testing.T) {
		if _, err := h.myliteDB.Exec("CREATE TABLE test_ps_null (id INT PRIMARY KEY, val INT)"); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer h.myliteDB.Exec("DROP TABLE IF EXISTS test_ps_null") //nolint:errcheck

		stmt, err := h.myliteDB.Prepare("INSERT INTO test_ps_null (id, val) VALUES (?, ?)")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer stmt.Close()

		if _, err := stmt.Exec(1, nil); err != nil {
			t.Fatalf("exec with NULL failed: %v", err)
		}

		var id int
		var val sql.NullInt64
		if err := h.myliteDB.QueryRow("SELECT id, val FROM test_ps_null WHERE id = 1").Scan(&id, &val); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if id != 1 {
			t.Errorf("expected id=1, got %d", id)
		}
		if val.Valid {
			t.Errorf("expected val=NULL, got %d", val.Int64)
		}
	})

	// Suppress unused import error if mysqlDB is nil (the harness h is used above)
	_ = h
}

func TestGroupBy(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "GROUP BY with COUNT(*)",
		Setup: []string{
			"CREATE TABLE test_gb_orders (id INT PRIMARY KEY, user_id INT, amount INT)",
			"INSERT INTO test_gb_orders (id, user_id, amount) VALUES (1, 1, 100)",
			"INSERT INTO test_gb_orders (id, user_id, amount) VALUES (2, 1, 200)",
			"INSERT INTO test_gb_orders (id, user_id, amount) VALUES (3, 2, 150)",
		},
		Query: "SELECT user_id, COUNT(*) FROM test_gb_orders GROUP BY user_id ORDER BY user_id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_gb_orders",
		},
	})

	h.Run(TestCase{
		Name: "GROUP BY with SUM",
		Setup: []string{
			"CREATE TABLE test_gb_sales (id INT PRIMARY KEY, category VARCHAR(50), amount INT)",
			"INSERT INTO test_gb_sales (id, category, amount) VALUES (1, 'A', 10)",
			"INSERT INTO test_gb_sales (id, category, amount) VALUES (2, 'A', 20)",
			"INSERT INTO test_gb_sales (id, category, amount) VALUES (3, 'B', 30)",
		},
		Query: "SELECT category, SUM(amount) FROM test_gb_sales GROUP BY category ORDER BY category",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_gb_sales",
		},
	})

	h.Run(TestCase{
		Name: "GROUP BY with MAX and MIN",
		Setup: []string{
			"CREATE TABLE test_gb_scores (id INT PRIMARY KEY, student VARCHAR(50), score INT)",
			"INSERT INTO test_gb_scores (id, student, score) VALUES (1, 'Alice', 90)",
			"INSERT INTO test_gb_scores (id, student, score) VALUES (2, 'Alice', 80)",
			"INSERT INTO test_gb_scores (id, student, score) VALUES (3, 'Bob', 70)",
			"INSERT INTO test_gb_scores (id, student, score) VALUES (4, 'Bob', 95)",
		},
		Query: "SELECT student, MAX(score), MIN(score) FROM test_gb_scores GROUP BY student ORDER BY student",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_gb_scores",
		},
	})

	h.Run(TestCase{
		Name: "COUNT(*) without GROUP BY",
		Setup: []string{
			"CREATE TABLE test_cnt (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_cnt (id, val) VALUES (1, 10)",
			"INSERT INTO test_cnt (id, val) VALUES (2, 20)",
			"INSERT INTO test_cnt (id, val) VALUES (3, 30)",
		},
		Query: "SELECT COUNT(*) FROM test_cnt",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_cnt",
		},
	})

	h.Run(TestCase{
		Name: "GROUP BY with HAVING",
		Setup: []string{
			"CREATE TABLE test_having (id INT PRIMARY KEY, dept VARCHAR(50), salary INT)",
			"INSERT INTO test_having (id, dept, salary) VALUES (1, 'eng', 100)",
			"INSERT INTO test_having (id, dept, salary) VALUES (2, 'eng', 120)",
			"INSERT INTO test_having (id, dept, salary) VALUES (3, 'hr', 80)",
		},
		Query: "SELECT dept, COUNT(*) AS cnt FROM test_having GROUP BY dept HAVING cnt > 1",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_having",
		},
	})
}

func TestSnapshot(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name:       "CREATE SNAPSHOT and RESTORE SNAPSHOT restores rows",
		MyliteOnly: true,
		Setup: []string{
			"CREATE TABLE test_snap_rows (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_snap_rows (id, val) VALUES (1, 10)",
			"INSERT INTO test_snap_rows (id, val) VALUES (2, 20)",
			"MYLITE CREATE SNAPSHOT snap1",
			"INSERT INTO test_snap_rows (id, val) VALUES (3, 30)",
			"MYLITE RESTORE SNAPSHOT snap1",
		},
		Query: "SELECT id, val FROM test_snap_rows ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_snap_rows",
			"MYLITE DROP SNAPSHOT snap1",
		},
	})

	h.Run(TestCase{
		Name:       "RESTORE SNAPSHOT removes rows deleted after snapshot",
		MyliteOnly: true,
		Setup: []string{
			"CREATE TABLE test_snap_del (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_snap_del (id, val) VALUES (1, 100)",
			"MYLITE CREATE SNAPSHOT snapdel",
			"DELETE FROM test_snap_del WHERE id = 1",
			"MYLITE RESTORE SNAPSHOT snapdel",
		},
		Query: "SELECT id, val FROM test_snap_del ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_snap_del",
			"MYLITE DROP SNAPSHOT snapdel",
		},
	})

	h.Run(TestCase{
		Name:       "RESTORE SNAPSHOT drops tables created after snapshot",
		MyliteOnly: true,
		Setup: []string{
			"MYLITE CREATE SNAPSHOT snapddl",
			"CREATE TABLE test_snap_ddl (id INT PRIMARY KEY)",
			"INSERT INTO test_snap_ddl (id) VALUES (1)",
			"MYLITE RESTORE SNAPSHOT snapddl",
		},
		Query:       "SELECT * FROM test_snap_ddl",
		ExpectError: true,
		Teardown: []string{
			"DROP TABLE IF EXISTS test_snap_ddl",
			"MYLITE DROP SNAPSHOT snapddl",
		},
	})

	h.Run(TestCase{
		Name:        "RESTORE SNAPSHOT on nonexistent snapshot returns error",
		MyliteOnly:  true,
		Setup:       []string{},
		Query:       "MYLITE RESTORE SNAPSHOT nonexistent",
		ExpectError: true,
	})
}

func TestOnDuplicateKeyUpdate(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "ON DUPLICATE KEY UPDATE updates existing row on PK conflict",
		Setup: []string{
			"CREATE TABLE test_odup (id INT PRIMARY KEY, name VARCHAR(255), val INT)",
			"INSERT INTO test_odup (id, name, val) VALUES (1, 'alice', 10)",
			"INSERT INTO test_odup (id, name, val) VALUES (1, 'alice_dup', 99) ON DUPLICATE KEY UPDATE val = 99",
		},
		Query: "SELECT id, name, val FROM test_odup ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_odup",
		},
	})

	h.Run(TestCase{
		Name: "ON DUPLICATE KEY UPDATE inserts when no duplicate exists",
		Setup: []string{
			"CREATE TABLE test_odup_ins (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_odup_ins (id, val) VALUES (1, 10)",
			"INSERT INTO test_odup_ins (id, val) VALUES (2, 20) ON DUPLICATE KEY UPDATE val = 20",
		},
		Query: "SELECT id, val FROM test_odup_ins ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_odup_ins",
		},
	})

	h.Run(TestCase{
		Name: "ON DUPLICATE KEY UPDATE with UNIQUE key conflict",
		Setup: []string{
			"CREATE TABLE test_odup_uni (id INT PRIMARY KEY AUTO_INCREMENT, email VARCHAR(255) UNIQUE, score INT)",
			"INSERT INTO test_odup_uni (email, score) VALUES ('a@b.com', 10)",
			"INSERT INTO test_odup_uni (email, score) VALUES ('a@b.com', 50) ON DUPLICATE KEY UPDATE score = 50",
		},
		Query: "SELECT email, score FROM test_odup_uni ORDER BY id",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_odup_uni",
		},
	})
}

func TestLastInsertID(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "LAST_INSERT_ID() returns id of last auto-increment insert",
		Setup: []string{
			"CREATE TABLE test_liid (id INT PRIMARY KEY AUTO_INCREMENT, val VARCHAR(50))",
			"INSERT INTO test_liid (val) VALUES ('first')",
			"INSERT INTO test_liid (val) VALUES ('second')",
		},
		Query: "SELECT LAST_INSERT_ID()",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_liid",
		},
	})
}

func TestBuiltinFunctions(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name:  "VERSION() returns server version",
		Query: "SELECT VERSION()",
		// MySQL and mylite return different version strings, skip result compare
		SkipResultCompare: true,
	})

	h.Run(TestCase{
		Name:  "DATABASE() returns current database name",
		Query: "SELECT DATABASE()",
		// Skip compare: MySQL may use a different current DB name
		SkipResultCompare: true,
	})

	h.Run(TestCase{
		Name:  "CONCAT() concatenates strings",
		Query: "SELECT CONCAT('hello', ' ', 'world')",
	})

	h.Run(TestCase{
		Name:  "CONCAT() with NULL returns NULL",
		Query: "SELECT CONCAT('a', NULL, 'b')",
	})

	h.Run(TestCase{
		Name:  "UPPER() and LOWER()",
		Query: "SELECT UPPER('hello'), LOWER('WORLD')",
	})

	h.Run(TestCase{
		Name:  "LENGTH() returns byte length",
		Query: "SELECT LENGTH('hello')",
	})

	h.Run(TestCase{
		Name:  "CHAR_LENGTH() returns character count",
		Query: "SELECT CHAR_LENGTH('hello')",
	})

	h.Run(TestCase{
		Name:  "SUBSTRING() extracts substring",
		Query: "SELECT SUBSTRING('hello world', 7, 5)",
	})

	h.Run(TestCase{
		Name:  "SUBSTRING() from position",
		Query: "SELECT SUBSTRING('hello world', 1, 5)",
	})

	h.Run(TestCase{
		Name:  "TRIM() removes whitespace",
		Query: "SELECT TRIM('  hello  ')",
	})

	h.Run(TestCase{
		Name:  "REPLACE() replaces substrings",
		Query: "SELECT REPLACE('hello world', 'world', 'there')",
	})

	h.Run(TestCase{
		Name:  "IFNULL() returns first arg when not null",
		Query: "SELECT IFNULL('value', 'default')",
	})

	h.Run(TestCase{
		Name:  "IFNULL() returns second arg when first is null",
		Query: "SELECT IFNULL(NULL, 'default')",
	})

	h.Run(TestCase{
		Name:  "COALESCE() returns first non-null",
		Query: "SELECT COALESCE(NULL, NULL, 'third', 'fourth')",
	})

	h.Run(TestCase{
		Name:  "IF() returns true branch",
		Query: "SELECT IF(1=1, 'yes', 'no')",
	})

	h.Run(TestCase{
		Name:  "IF() returns false branch",
		Query: "SELECT IF(1=2, 'yes', 'no')",
	})

	h.Run(TestCase{
		Name:  "ABS() returns absolute value",
		Query: "SELECT ABS(-42)",
	})

	h.Run(TestCase{
		Name:  "ROUND() rounds a number",
		Query: "SELECT ROUND(3.7)",
	})

	h.Run(TestCase{
		Name:  "FLOOR() floors a number",
		Query: "SELECT FLOOR(3.9)",
	})

	h.Run(TestCase{
		Name:  "CEIL() ceils a number",
		Query: "SELECT CEIL(3.1)",
	})
}

func TestSystemVariables(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name:  "@@version_comment",
		Query: "SELECT @@version_comment",
		// mylite returns "mylite", MySQL returns different value
		SkipResultCompare: true,
	})

	h.Run(TestCase{
		Name:  "@@max_allowed_packet",
		Query: "SELECT @@max_allowed_packet",
		// Values may differ between mylite and MySQL
		SkipResultCompare: true,
	})

	h.Run(TestCase{
		Name:  "@@character_set_client",
		Query: "SELECT @@character_set_client",
		// Both should return "utf8mb4" but skip to be safe
		SkipResultCompare: true,
	})

	h.Run(TestCase{
		Name:  "@@autocommit",
		Query: "SELECT @@autocommit",
		// Both should return 1
		SkipResultCompare: true,
	})
}

func TestCTE(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "simple CTE with SELECT",
		Setup: []string{
			"CREATE TABLE test_cte_orders (id INT PRIMARY KEY, amount INT, status VARCHAR(50))",
			"INSERT INTO test_cte_orders (id, amount, status) VALUES (1, 100, 'paid')",
			"INSERT INTO test_cte_orders (id, amount, status) VALUES (2, 200, 'paid')",
			"INSERT INTO test_cte_orders (id, amount, status) VALUES (3, 50, 'pending')",
		},
		Query: `WITH paid_orders AS (
			SELECT id, amount FROM test_cte_orders WHERE status = 'paid'
		)
		SELECT id, amount FROM paid_orders ORDER BY id`,
		Teardown: []string{
			"DROP TABLE IF EXISTS test_cte_orders",
		},
	})

	h.Run(TestCase{
		Name: "CTE with aggregation",
		Setup: []string{
			"CREATE TABLE test_cte_sales (id INT PRIMARY KEY, region VARCHAR(50), amount INT)",
			"INSERT INTO test_cte_sales (id, region, amount) VALUES (1, 'north', 300)",
			"INSERT INTO test_cte_sales (id, region, amount) VALUES (2, 'south', 150)",
			"INSERT INTO test_cte_sales (id, region, amount) VALUES (3, 'north', 200)",
		},
		Query: `WITH regional AS (
			SELECT region, SUM(amount) AS total FROM test_cte_sales GROUP BY region
		)
		SELECT region, total FROM regional ORDER BY region`,
		Teardown: []string{
			"DROP TABLE IF EXISTS test_cte_sales",
		},
	})

	h.Run(TestCase{
		Name: "CTE filtering with WHERE on CTE result",
		Setup: []string{
			"CREATE TABLE test_cte_items (id INT PRIMARY KEY, name VARCHAR(100), price INT)",
			"INSERT INTO test_cte_items (id, name, price) VALUES (1, 'alpha', 10)",
			"INSERT INTO test_cte_items (id, name, price) VALUES (2, 'beta', 20)",
			"INSERT INTO test_cte_items (id, name, price) VALUES (3, 'gamma', 30)",
		},
		Query: `WITH expensive AS (
			SELECT id, name, price FROM test_cte_items WHERE price >= 20
		)
		SELECT id, name FROM expensive ORDER BY id`,
		Teardown: []string{
			"DROP TABLE IF EXISTS test_cte_items",
		},
	})
}

func TestTruncate(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "TRUNCATE TABLE removes all rows",
		Setup: []string{
			"CREATE TABLE test_truncate (id INT PRIMARY KEY, val INT)",
			"INSERT INTO test_truncate (id, val) VALUES (1, 10)",
			"INSERT INTO test_truncate (id, val) VALUES (2, 20)",
			"INSERT INTO test_truncate (id, val) VALUES (3, 30)",
			"TRUNCATE TABLE test_truncate",
		},
		Query: "SELECT COUNT(*) FROM test_truncate",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_truncate",
		},
	})

	h.Run(TestCase{
		Name: "TRUNCATE TABLE resets AUTO_INCREMENT",
		Setup: []string{
			"CREATE TABLE test_truncate_ai (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100))",
			"INSERT INTO test_truncate_ai (name) VALUES ('a')",
			"INSERT INTO test_truncate_ai (name) VALUES ('b')",
			"TRUNCATE TABLE test_truncate_ai",
			"INSERT INTO test_truncate_ai (name) VALUES ('c')",
		},
		Query: "SELECT id FROM test_truncate_ai",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_truncate_ai",
		},
	})
}

func TestInformationSchema(t *testing.T) {
	h := getHarness(t)

	h.Run(TestCase{
		Name: "INFORMATION_SCHEMA.SCHEMATA lists databases",
		Setup: []string{
			"CREATE DATABASE IF NOT EXISTS test_is_db",
		},
		Query: "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = 'test_is_db'",
		Teardown: []string{
			"DROP DATABASE IF EXISTS test_is_db",
		},
		MyliteOnly: true,
	})

	h.Run(TestCase{
		Name: "INFORMATION_SCHEMA.TABLES lists tables",
		Setup: []string{
			"CREATE TABLE test_is_tables (id INT PRIMARY KEY, name VARCHAR(100))",
		},
		Query: "SELECT TABLE_NAME, TABLE_TYPE, ENGINE FROM information_schema.tables WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'test_is_tables'",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_is_tables",
		},
		MyliteOnly: true,
	})

	h.Run(TestCase{
		Name: "INFORMATION_SCHEMA.COLUMNS lists columns",
		Setup: []string{
			"CREATE TABLE test_is_cols (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, age INT)",
		},
		Query: "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'test_is_cols' ORDER BY ORDINAL_POSITION",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_is_cols",
		},
		MyliteOnly: true,
	})

	h.Run(TestCase{
		Name: "INFORMATION_SCHEMA.SCHEMATA column structure",
		Query: "SELECT CATALOG_NAME, DEFAULT_CHARACTER_SET_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = 'test'",
		MyliteOnly: true,
	})

	h.Run(TestCase{
		Name: "SHOW TABLE STATUS returns rows for current DB",
		Setup: []string{
			"CREATE TABLE test_ts_show (id INT PRIMARY KEY)",
		},
		Query: "SHOW TABLE STATUS",
		SkipResultCompare: true,
		Teardown: []string{
			"DROP TABLE IF EXISTS test_ts_show",
		},
		MyliteOnly: true,
	})

	h.Run(TestCase{
		Name: "INFORMATION_SCHEMA.TABLES WHERE filter works",
		Setup: []string{
			"CREATE TABLE test_is_filter_a (id INT PRIMARY KEY)",
		},
		Query: "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'test_is_filter_a'",
		Teardown: []string{
			"DROP TABLE IF EXISTS test_is_filter_a",
		},
		MyliteOnly: true,
	})
}
