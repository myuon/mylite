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
go run ./cmd/mtrrun -verbose

# テスト結果のサマリのみ
go run ./cmd/mtrrun
```

## mtrrunの使い方（追加オプション）

```bash
# 全スイート実行
go run ./cmd/mtrrun -verbose

# 特定スイートのみ実行
go run ./cmd/mtrrun -suite sys_vars
go run ./cmd/mtrrun -suite sys_vars,innodb

# 特定テストのみ実行
go run ./cmd/mtrrun -test sys_vars/gtid_owned_basic
go run ./cmd/mtrrun -test sys_vars/gtid_owned_basic,other/bool

# skipされたテストのみ実行（skiplistを変更せずに確認）
go run ./cmd/mtrrun -suite sys_vars -skipped-only -force
```

実行結果は `.mtrrun-logs/result-YYYYMMDD-HHMMSS.json` に自動保存される。

mtrrunはstdoutにGrand TotalとJSONログパスのみ出力する。テスト結果の詳細はJSONログを参照すること。
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
