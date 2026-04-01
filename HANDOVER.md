# Handover: mylite MTR テスト改善作業

## 現在の状態

- 通過テスト数: **1437 / 3345**（skiplist: 1926件）
- ブランチ: `main`
- 直前のコミット: `cd16c85` — mtrrun同時実行ロック追加

---

## 完了済みの作業（今セッション）

| コミット | 内容 |
|---------|------|
| `cd16c85` | mtrrun: `/tmp/mtrrun.lock` によるPIDロックで同時実行禁止 |
| `c839743` | 7テスト追加修正 (累計1437通過) |
| `63073bb` | sys_vars 6テスト unskip |
| `3a18e8b` | システム変数デフォルト値・範囲・スコープ修正 |
| `9a731ae` | strictモード・ENUM・trailing space修正 |
| `9a95f54` | カラムケース・charset検証・SHOW CREATE TABLE・ODBC構文 |

---

## 次にやること（優先順）

### 1. UUID/conditional comment パースエラー修正（ほぼ完成済み）

worktree `agent-a747c96030012e24b` でエージェントが実装済みだが未マージ。

**変更内容:**
- `executor/procedures.go`: `substituteLocalVars` でstring値をクォート、`replaceWordBoundary` でドットを単語境界として扱う
- `mtrrunner/runner.go`: `--query_vertical` がインラインSQLのとき複数行収集に入らない

**マージ手順:**
```bash
# worktreeの場所を確認
git worktree list

# 変更を確認
git -C <worktree-path> diff HEAD~1

# mainのファイルを読んで差分をEditツールで手動適用（patch -p0は使わない）
# → go build ./... && go test ./... -count=1 -timeout 60s で検証
```

### 2. sys_vars バリデーション修正（約33テスト）

失敗パターン: 数値範囲チェック・型変換エラー

```bash
go run ./cmd/mtrrun -suite sys_vars 2>&1 | grep FAIL | head -20
```

### 3. アウトプットミスマッチ修正（約162テスト）

各スイートの FAIL を個別に `-test` フラグで再現してから修正する方針。

```bash
# 例: innodb スイートの失敗を確認
go run ./cmd/mtrrun -suite innodb 2>&1 | grep "FAIL:"
```

---

## 重要な運用ルール

- **skiplist.json は直接編集禁止** → `python3 scripts/skiplist.py` 経由のみ
- **worktreeマージはpatch不可** → EditツールでRead→差分確認→手動適用
- **エージェント並列実行時は `isolation: "worktree"` を指定**
- **mtrrun同時実行は `$TMPDIR/mtrrun.lock` でブロックされる**（手動削除: `rm $TMPDIR/mtrrun.lock`）
- **mtrrun単体テスト実行**: `go run ./cmd/mtrrun -test sys_vars/foo_basic`（~0.3秒）

---

## skiplistの構造

`cmd/mtrrun/skiplist.json` — 1926エントリ。reasonフィールドはスイート単位の共有文字列なので信頼しない。実際に `-test` で実行して確認すること。

```bash
# 特定テストがなぜ失敗するか確認
go run ./cmd/mtrrun -test sys_vars/foo_basic -verbose

# skiplistから除外（通過するようになったら）
python3 scripts/skiplist.py remove sys_vars/foo_basic
```

---

## ツール・コマンドリファレンス

```bash
go run ./cmd/mtrrun                          # 全スイート実行
go run ./cmd/mtrrun -suite sys_vars          # 特定スイート
go run ./cmd/mtrrun -test sys_vars/foo       # 特定テスト
go run ./cmd/mtrrun -verbose                 # 詳細出力

python3 scripts/skiplist.py count sys_vars   # スイートのskip数確認
python3 scripts/skiplist.py validate         # 存在しないテストのエントリ検出
python3 scripts/skiplist.py remove foo/bar   # skip解除

go test ./... -count=1 -timeout 60s          # ユニットテスト全実行
go build ./...                               # ビルド確認
```
