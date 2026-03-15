.PHONY: build test test-compat mysql-up mysql-down

build:
	go build ./...

test:
	go test ./harness/ -v -count=1

# Start MySQL 8.4 for comparison testing
mysql-up:
	docker compose up -d mysql
	@echo "Waiting for MySQL to be ready..."
	@until docker compose exec mysql mysqladmin ping -h localhost --silent 2>/dev/null; do sleep 1; done
	@echo "MySQL is ready on port 13306"

mysql-down:
	docker compose down

# Run tests with MySQL 8.4 comparison
test-compat: mysql-up
	MYSQL_TEST_DSN="root:@tcp(127.0.0.1:13306)/test" go test ./harness/ -v -count=1

run:
	go run ./cmd/mylite -addr :3307
