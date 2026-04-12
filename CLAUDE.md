# mylite

MySQL互換の軽量データベースエンジン（Go実装）。

## ビルド・テスト

```bash
go build ./...
go test ./... -count=1
```

## MySQLテストスイート (mtrrun)

```bash
# 全スイート実行
go run ./cmd/mtrrun

# 特定スイートのみ実行
go run ./cmd/mtrrun -suite sys_vars
go run ./cmd/mtrrun -suite sys_vars,innodb

# 特定テストのみ実行
go run ./cmd/mtrrun -test sys_vars/gtid_owned_basic
go run ./cmd/mtrrun -test sys_vars/gtid_owned_basic,other/bool

# skipされたテストのみ実行（skiplistを変更せずに確認）
go run ./cmd/mtrrun -suite sys_vars -skipped-only -force
```

stdoutにはサマリとログパスのみ出力される:
```
=== Grand Total ===
Suites: 29, Total: 3345, Passed: 1553, Failed: 367, Skipped: 1227, Errors: 191, Timeouts: 2
Time: 143.6s
Results saved to: .mtrrun-logs/result-20260406-162232.json
```

テスト結果の詳細は `.mtrrun-logs/` のJSONログを参照すること。
```bash
# 最新のログを確認
ls -t .mtrrun-logs/ | head -1

# FAILテストの一覧
jq -r '.tests[] | select(.status=="fail") | .name' .mtrrun-logs/<latest>.json

# ERRORパターンの集計
jq -r '.tests[] | select(.status=="error") | .error' .mtrrun-logs/<latest>.json | sort | uniq -c | sort -rn

# 特定スイートのFAIL
jq -r '.tests[] | select(.suite=="other" and .status=="fail") | .name' .mtrrun-logs/<latest>.json
```

## skiplist操作

```bash
# テストをskiplistから除外
python3 scripts/skiplist.py remove sys_vars/foo_basic sys_vars/bar_basic

# テストをskiplistに追加
python3 scripts/skiplist.py add other/some_test "reason for skipping"

# スイートごとのエントリ数確認
python3 scripts/skiplist.py count sys_vars

# 存在しないテストファイルのエントリを検出
python3 scripts/skiplist.py validate
```

⚠️ エージェントにskiplist.jsonを直接編集させない。必ずスクリプト経由で操作すること。

## 機能追加・修正の進め方

### 方針
- テストのdiffを埋めるのではなく、MySQLの仕様に基づいて機能を正しく実装する
- エージェントには小スコープ・明確なタスクだけ渡す。修正箇所まで特定してから依頼すること
- 広いスコープ（例: 「sys_vars 101テスト修正」）は修正→regress→調査の泥沼になるのでNG

### KPI: first-diff-line
テスト改善の指標として **first-diff-line**（最初のdiffが発生する行番号）を使う。この値が大きいほど、テスト前半の多くの行が正しい出力と一致していることを示す。

diff行数はmtrrunの20ミスマッチ上限でカットされるため合計行数はKPIとして使えない。

```bash
# 修正前後のfirst-diff-line比較
python3 -c "
import json, re
for f in ['before.json', 'after.json']:
    with open(f'.mtrrun-logs/{f}') as fh:
        data = json.load(fh)
    for t in data['tests']:
        if t['status'] == 'fail' and t.get('diff'):
            m = re.match(r'line (\d+):', t['diff'])
            if m: print(f'{f}: {t[\"name\"]}: line {m.group(1)}')
"

# 特定テストのfirst-diff-line確認
jq -r '.tests[] | select(.name=="<test>") | .diff' .mtrrun-logs/<latest>.json | head -3
```

### 修正後の確認手順
1. `go build ./... && go test ./... -count=1` でビルド・ユニットテスト確認
2. `go run ./cmd/mtrrun` でfull suite実行
3. 修正前のresult JSONと比較して:
   - passが増えたテストがあるか
   - first-diff-lineが後退（改善）したテストがあるか
   - **regressionがないか**（passだったテストがfail/errorになっていないか）
4. regressionがあれば即revert
