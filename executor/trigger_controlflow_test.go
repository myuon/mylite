package executor

import (
	"strings"
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

func TestBeforeAfterUpdateTriggerNewOld(t *testing.T) {
	e := newTriggerTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE products (id INT PRIMARY KEY, price INT, old_price INT)"); err != nil {
		t.Fatalf("create products: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE audit (id INT PRIMARY KEY AUTO_INCREMENT, product_id INT, before_price INT, after_price INT)"); err != nil {
		t.Fatalf("create audit: %v", err)
	}

	// BEFORE UPDATE trigger: reads OLD, modifies NEW
	if _, err := e.Execute(`CREATE TRIGGER tr_before_update BEFORE UPDATE ON products FOR EACH ROW SET NEW.old_price = OLD.price`); err != nil {
		t.Fatalf("create before update trigger: %v", err)
	}
	// AFTER UPDATE trigger: inserts audit row with OLD and NEW values
	if _, err := e.Execute(`CREATE TRIGGER tr_after_update AFTER UPDATE ON products FOR EACH ROW INSERT INTO audit (product_id, before_price, after_price) VALUES (NEW.id, OLD.price, NEW.price)`); err != nil {
		t.Fatalf("create after update trigger: %v", err)
	}

	// Insert initial data
	if _, err := e.Execute("INSERT INTO products (id, price, old_price) VALUES (1, 100, 0)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Update price
	if _, err := e.Execute("UPDATE products SET price = 200 WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Check old_price was set by BEFORE trigger
	res, err := e.Execute("SELECT price, old_price FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("select products: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected 1 row in products")
	}
	if res.Rows[0][0] != int64(200) {
		t.Errorf("expected price=200, got %v", res.Rows[0][0])
	}
	if res.Rows[0][1] != int64(100) {
		t.Errorf("expected old_price=100 (set by BEFORE trigger from OLD.price), got %v", res.Rows[0][1])
	}

	// Check audit row was created by AFTER trigger
	res, err = e.Execute("SELECT before_price, after_price FROM audit WHERE product_id = 1")
	if err != nil {
		t.Fatalf("select audit: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected 1 audit row")
	}
	if res.Rows[0][0] != int64(100) {
		t.Errorf("expected before_price=100 (OLD.price), got %v", res.Rows[0][0])
	}
	if res.Rows[0][1] != int64(200) {
		t.Errorf("expected after_price=200 (NEW.price), got %v", res.Rows[0][1])
	}
}

func TestBeforeAfterDeleteTriggerOld(t *testing.T) {
	e := newTriggerTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50))"); err != nil {
		t.Fatalf("create items: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE deleted_items (id INT, name VARCHAR(50), deleted_at VARCHAR(50))"); err != nil {
		t.Fatalf("create deleted_items: %v", err)
	}

	// BEFORE DELETE trigger: logs the deletion
	if _, err := e.Execute(`CREATE TRIGGER tr_before_delete BEFORE DELETE ON items FOR EACH ROW SET @last_deleted = OLD.name`); err != nil {
		t.Fatalf("create before delete trigger: %v", err)
	}
	// AFTER DELETE trigger: archives deleted rows
	if _, err := e.Execute(`CREATE TRIGGER tr_after_delete AFTER DELETE ON items FOR EACH ROW INSERT INTO deleted_items (id, name) VALUES (OLD.id, OLD.name)`); err != nil {
		t.Fatalf("create after delete trigger: %v", err)
	}

	// Insert test data
	if _, err := e.Execute("INSERT INTO items (id, name) VALUES (1, 'Widget'), (2, 'Gadget')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Delete one row
	if _, err := e.Execute("DELETE FROM items WHERE id = 1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Check @last_deleted was set by BEFORE DELETE trigger
	res, err := e.Execute("SELECT @last_deleted")
	if err != nil {
		t.Fatalf("select @last_deleted: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != "Widget" {
		t.Errorf("expected @last_deleted='Widget', got %v", res.Rows)
	}

	// Check deleted_items was populated by AFTER DELETE trigger
	res, err = e.Execute("SELECT id, name FROM deleted_items")
	if err != nil {
		t.Fatalf("select deleted_items: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected 1 deleted_items row")
	}
	if res.Rows[0][0] != int64(1) || res.Rows[0][1] != "Widget" {
		t.Errorf("expected deleted row (1, Widget), got %v", res.Rows[0])
	}

	// Check items still has Gadget
	res, err = e.Execute("SELECT COUNT(*) FROM items")
	if err != nil {
		t.Fatalf("select count: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] != int64(1) {
		t.Errorf("expected 1 remaining item, got %v", res.Rows)
	}
}

func TestTriggerSetOldColError(t *testing.T) {
	e := newTriggerTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE t1 (id INT, val INT)"); err != nil {
		t.Fatalf("create t1: %v", err)
	}

	// SET OLD.col in any trigger should give error 1362 (ER_TRG_CANT_CHANGE_ROW)
	_, err := e.Execute("CREATE TRIGGER tr_bad BEFORE UPDATE ON t1 FOR EACH ROW SET OLD.val = 100")
	if err == nil {
		t.Fatal("expected error for SET OLD.col in trigger")
	}
	if !strings.Contains(err.Error(), "1362") {
		t.Errorf("expected error 1362 (Updating of OLD row), got: %v", err)
	}

	// SET OLD.col in AFTER INSERT should also give 1362
	_, err = e.Execute("CREATE TRIGGER tr_bad2 AFTER INSERT ON t1 FOR EACH ROW SET OLD.val = 100")
	if err == nil {
		t.Fatal("expected error for SET OLD.col in AFTER INSERT trigger")
	}
	if !strings.Contains(err.Error(), "1362") {
		t.Errorf("expected error 1362 (Updating of OLD row), got: %v", err)
	}

	// Reading OLD.col in INSERT trigger should give 1363 (ER_TRG_NO_SUCH_ROW_IN_TRG)
	_, err = e.Execute("CREATE TRIGGER tr_bad3 BEFORE INSERT ON t1 FOR EACH ROW SET @x = OLD.val")
	if err == nil {
		t.Fatal("expected error for reading OLD.col in INSERT trigger")
	}
	if !strings.Contains(err.Error(), "1363") {
		t.Errorf("expected error 1363 (no OLD row in INSERT), got: %v", err)
	}
}
