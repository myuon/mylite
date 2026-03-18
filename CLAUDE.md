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
