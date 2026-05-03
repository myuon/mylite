# Close-to-pass test candidates (mtrrun)

Snapshot taken from `.mtrrun-logs/result-20260503-061456.json` (passed=1811,
failed=330, error=123, skip=1080). Tests below have **small diffs** but each
requires non-trivial work. Recorded here so future agents can avoid re-doing the
same scoping work.

## Top 5 candidates with brief diagnosis

### 1. `other/condition_filter` — diff 4 lines
- **First diff line 68**: `Sort_rows  1000` (expected) vs `Sort_rows  20` (actual).
- **Cause**: `executor/select.go:3206` increments `e.sortRows` AFTER `LIMIT` is
  applied. MySQL counts rows in the *filesort* buffer of the driving table
  *before* the join. For `SELECT ... FROM t1 JOIN t2 ... ORDER BY t1.i1 LIMIT 20`
  MySQL filesorts all 1000 t1 rows first, joins each, then takes 20.
- **Fix sketch**: Detect "single-table TOP-N filesort" vs "join driving sort"
  and increment `sortRows` to either `LIMIT` or driving-table size accordingly.
  Counting *before* `LIMIT` would also break `other/order_by_sortkey`
  (expects `Sort_rows=100` for `SELECT ... FROM t1 ORDER BY ... LIMIT 100`).
- **Risk**: Medium — single-table case currently passes by counting after LIMIT.

### 2. `other/parser_stack` — diff 7 lines
- **First diff line 145**: `10  16` (expected) vs `10  NULL` (actual).
- **Cause**: Trigger body containing 108-deep nested `BEGIN ... END` with
  `SET NEW.b := NEW.a; SET NEW.b := NEW.b + 1; SET NEW.b := NEW.b + 2; ...`.
  Our trigger executor returns `NEW.b = NULL`, suggesting either parser drops
  the inner `SET` statements or trigger body skips them in deeply nested blocks.
- **Fix sketch**: Investigate `executor/procedures.go` block-flattening for
  triggers with deeply nested BEGIN/END. Confirm `SET NEW.col = ...` statements
  inside many-level-nested compound blocks execute.
- **Risk**: Medium — could regress other trigger tests.

### 3. `other/type_decimal` — diff 7 lines
- **First diff line 863**: `300.00` then `201.11` (expected) vs reversed.
- **Cause**: `select max(case 1 when 1 then c else null end) from t1 group by c;`
  Expected output is in *insertion order* of the group keys (300 inserted first,
  then 201.11). Our GROUP BY emits sorted ascending order (201.11 then 300.00).
- **Fix sketch**: Preserve insertion order in GROUP BY when no ORDER BY. MySQL
  8.0 docs say GROUP BY has *no implicit sort* but real MySQL emits in some
  consistent order (often hash-table iteration). Hard to match exactly.
- **Risk**: High — could regress many GROUP BY tests that depend on current
  sort order.

### 4. `json/json_innodb` — FIXED
- Root cause was in `syntheticTableDefFromViewSQL` (`executor/select.go`):
  when a view's SELECT mixed an unnamed computed expression with a direct
  column ref (e.g. `SELECT SUM(col_int), col_int FROM ...`), the computed
  expr was silently dropped, leaving the synthetic view def with one fewer
  column. `SELECT * FROM view` then returned only `col_int`, hiding the
  `SUM(col_int)` column entirely. Not actually JSON-related.
- Fix: bail out of the synthesizer when any expr lacks a clean name; let
  the caller fall back to the actual view-scan column names (which preserve
  MySQL's exact display style like `SUM(col_int)` and `b+1`).

### 5. `other/endspace` — diff 55 lines
- **First diff line 8**: `1  0  0` (expected) vs `0  1  0` (actual) for
  `select 'a\0' = 'a', 'a\0' < 'a', 'a\0' > 'a';` under
  `set names utf8mb4 collate utf8mb4_unicode_ci`.
- **Cause**: PAD_SPACE collation should treat trailing `\0` (NUL) as ignorable,
  making `'a\0' = 'a'` true. We treat `\0` as a smaller character.
- **Fix sketch**: Add `\0` (and other ignorable codepoints) to the trailing-char
  trimming logic for PAD_SPACE collations. Touch `executor/collation.go` or the
  comparator under `executor/compare.go`.
- **Risk**: Medium — comparator changes can ripple through many sort/equality
  paths. Run the full collations suite as a regression check.

## Categories explicitly out-of-scope (do not pursue)

These categories have been tried multiple times by previous agents and are
documented as not worth further investment:

- `subquery_sj_*` (semijoin materialization EXPLAIN judgment)
- `instant_add_column_*`
- `explain_json_*`
- MyISAM-specific tests
- GIS / spatial reference systems
- `performance_schema` counter values (transient)
- `GRANT` / `REVOKE` partial-privilege semantics
- charset/collation default differences (utf8 vs utf8mb4 alias)

## Other observations

- `other/sort_buffer_size_functionality` (diff 7) and `other/wl6301_3` (diff 7)
  fail at line 1 because they shell out to `$MYSQL` / `$MYSQLADMIN` which we
  don't always have wired up; not actually close-to-pass.
- `other/mysqladmin_shutdown`, `other/mysqldump_gtid`, `other/mysqld--defaults-file`,
  `other/perror`, `other/file_contents` all rely on external binaries
  (`mysqladmin`, `mysqldump`, `mysqld`, `perror`) and are excluded from quick
  wins.
- `other/bug47671` (utf8 vs utf8mb4 charset alias display) is fixable but the
  test invokes the live `mysql -e "status"` command output which depends on
  the local MySQL client version.
