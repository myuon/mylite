package harness

import (
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
		Name: "ROLLBACK after CREATE TABLE removes table",
		Setup: []string{
			"BEGIN",
			"CREATE TABLE test_tx_ddl (id INT PRIMARY KEY)",
			"ROLLBACK",
		},
		// After rollback the table should not exist; querying it should error.
		Query:       "SELECT * FROM test_tx_ddl",
		ExpectError: true,
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
