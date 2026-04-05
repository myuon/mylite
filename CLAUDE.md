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

## agmux codexセッションの使い方

codex providerのセッションはエージェントとして使える。

```bash
# セッション作成
agmux session create <name> --provider codex -p /Users/ioijoi/ghq/github.com/myuon/mylite -m '初回の指示'

# 指示を送る（stoppedでもsendで再開できる。毎回新規作成しない）
agmux session send <id> '追加の指示'

# セッション一覧
agmux session list

# 会話ログ確認
agmux logs <id>
agmux logs <id> -n 40   # 行数指定
```

### 運用ルール

- stoppedセッションには `send` で再指示すれば再開できる。毎回新規作成しない。
- 進捗確認時は `agmux logs <id>` で会話ログを確認し、何をやったか・どこで詰まったかを把握してから追加指示を送る。
- codexの変更は未コミットで残ることが多いので、`git diff` で確認→ `go build` / `go test` → コミット＆プッシュの流れで拾う。

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

## worktreeマージの注意点

- worktreeは古いbaseから分岐するため、mainの最新コードを消すリスクがある
- 変更が小さい場合（1-2ファイル、数箇所）はworktreeを使わず直接Editが安全
- worktreeを使った場合、マージは以下の手順:
  1. mainのファイルをReadで読む
  2. worktreeのファイルをReadで読む
  3. 差分を理解し、Editで追加部分のみ適用
  4. `go build ./...` && `go test ./... -count=1 -timeout 60s` で検証
