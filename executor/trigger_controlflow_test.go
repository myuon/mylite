package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func newTriggerTestExecutor(t *testing.T) *Executor {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	return e
}

func TestTriggerWithIfThen(t *testing.T) {
	e := newTriggerTestExecutor(t)

	// Create tables
	if _, err := e.Execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status VARCHAR(20))"); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE audit_log (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT, msg VARCHAR(100))"); err != nil {
		t.Fatalf("create audit_log: %v", err)
	}

	// Create trigger with IF/THEN control flow
	triggerSQL := `CREATE TRIGGER tr_order_insert AFTER INSERT ON orders FOR EACH ROW BEGIN IF NEW.amount > 100 THEN INSERT INTO audit_log (order_id, msg) VALUES (NEW.id, 'large order'); END IF; END`
	if _, err := e.Execute(triggerSQL); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	// Insert a small order (should NOT create audit log)
	if _, err := e.Execute("INSERT INTO orders (id, amount, status) VALUES (1, 50, 'pending')"); err != nil {
		t.Fatalf("insert small order: %v", err)
	}

	res, err := e.Execute("SELECT COUNT(*) FROM audit_log")
	if err != nil {
		t.Fatalf("select count: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(0) {
		t.Errorf("expected 0 audit_log rows for small order, got %v", res.Rows[0][0])
	}

	// Insert a large order (SHOULD create audit log)
	if _, err := e.Execute("INSERT INTO orders (id, amount, status) VALUES (2, 200, 'pending')"); err != nil {
		t.Fatalf("insert large order: %v", err)
	}

	res, err = e.Execute("SELECT COUNT(*) FROM audit_log")
	if err != nil {
		t.Fatalf("select count: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(1) {
		t.Errorf("expected 1 audit_log row for large order, got %v", res.Rows[0][0])
	}
}

func TestTriggerWithIfThenBeforeSetNew(t *testing.T) {
	e := newTriggerTestExecutor(t)

	// Create table
	if _, err := e.Execute("CREATE TABLE items (id INT PRIMARY KEY, price INT, discount_applied VARCHAR(3))"); err != nil {
		t.Fatalf("create items: %v", err)
	}

	// Create BEFORE INSERT trigger with IF/THEN that modifies NEW row
	triggerSQL := `CREATE TRIGGER tr_item_discount BEFORE INSERT ON items FOR EACH ROW BEGIN IF NEW.price > 100 THEN SET NEW.discount_applied = 'yes'; ELSE SET NEW.discount_applied = 'no'; END IF; END`
	if _, err := e.Execute(triggerSQL); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	// Insert cheap item
	if _, err := e.Execute("INSERT INTO items (id, price, discount_applied) VALUES (1, 50, '')"); err != nil {
		t.Fatalf("insert cheap item: %v", err)
	}

	res, err := e.Execute("SELECT discount_applied FROM items WHERE id = 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != "no" {
		t.Errorf("expected discount_applied='no' for cheap item, got %v", res.Rows[0][0])
	}

	// Insert expensive item
	if _, err := e.Execute("INSERT INTO items (id, price, discount_applied) VALUES (2, 200, '')"); err != nil {
		t.Fatalf("insert expensive item: %v", err)
	}

	res, err = e.Execute("SELECT discount_applied FROM items WHERE id = 2")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != "yes" {
		t.Errorf("expected discount_applied='yes' for expensive item, got %v", res.Rows[0][0])
	}
}

func TestTriggerWithDeclareAndIf(t *testing.T) {
	e := newTriggerTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE products (id INT PRIMARY KEY, price INT, category VARCHAR(20))"); err != nil {
		t.Fatalf("create products: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE price_changes (id INT PRIMARY KEY AUTO_INCREMENT, product_id INT, note VARCHAR(100))"); err != nil {
		t.Fatalf("create price_changes: %v", err)
	}

	// Trigger with DECLARE and IF
	triggerSQL := `CREATE TRIGGER tr_price_check AFTER INSERT ON products FOR EACH ROW BEGIN DECLARE threshold INT DEFAULT 500; IF NEW.price > threshold THEN INSERT INTO price_changes (product_id, note) VALUES (NEW.id, 'premium'); END IF; END`
	if _, err := e.Execute(triggerSQL); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	// Insert cheap product
	if _, err := e.Execute("INSERT INTO products (id, price, category) VALUES (1, 100, 'basic')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	res, err := e.Execute("SELECT COUNT(*) FROM price_changes")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(0) {
		t.Errorf("expected 0 rows, got %v", res.Rows[0][0])
	}

	// Insert expensive product
	if _, err := e.Execute("INSERT INTO products (id, price, category) VALUES (2, 1000, 'luxury')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	res, err = e.Execute("SELECT COUNT(*) FROM price_changes")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(1) {
		t.Errorf("expected 1 row, got %v", res.Rows[0][0])
	}
}

func TestTriggerWithIfThenStringValues(t *testing.T) {
	e := newTriggerTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20))"); err != nil {
		t.Fatalf("create users: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE admin_log (id INT PRIMARY KEY AUTO_INCREMENT, user_name VARCHAR(50), msg VARCHAR(100))"); err != nil {
		t.Fatalf("create admin_log: %v", err)
	}

	// Trigger that uses string NEW.col values in INSERT (tests proper SQL quoting)
	triggerSQL := `CREATE TRIGGER tr_admin_log AFTER INSERT ON users FOR EACH ROW BEGIN IF NEW.role = 'admin' THEN INSERT INTO admin_log (user_name, msg) VALUES (NEW.name, 'admin created'); END IF; END`
	if _, err := e.Execute(triggerSQL); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	// Insert regular user
	if _, err := e.Execute("INSERT INTO users (id, name, role) VALUES (1, 'Alice', 'user')"); err != nil {
		t.Fatalf("insert regular user: %v", err)
	}

	res, err := e.Execute("SELECT COUNT(*) FROM admin_log")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(0) {
		t.Errorf("expected 0 admin_log rows, got %v", res.Rows[0][0])
	}

	// Insert admin user
	if _, err := e.Execute("INSERT INTO users (id, name, role) VALUES (2, 'Bob', 'admin')"); err != nil {
		t.Fatalf("insert admin: %v", err)
	}

	res, err = e.Execute("SELECT user_name FROM admin_log")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != "Bob" {
		t.Errorf("expected user_name='Bob', got %v", res.Rows)
	}
}
