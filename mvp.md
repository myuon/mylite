# mylite MVP

いいと思います。
その場合は 「新しいDBを作る」 というより、

「テスト用途に特化した、MySQL互換の軽量実行系を作る」

と捉えるのが重要です。

狙うべきは SQLite のような手軽さですが、SQLite のような汎用 embedded DB を目指すと範囲が広すぎます。
なので最初から、

- 本番 MySQL の代替ではない
- テスト専用
- MySQL 互換は「実務で刺さる部分」優先
- 互換性の境界を明示する

という方針にした方が成功しやすいです。

---

## まず結論

おすすめの設計思想はこれです。

### コア方針

MySQL server の完全再実装はしない。 代わりに、

**「MySQL 互換フロントエンド + 軽量テスト向け実行エンジン」**

を作る。

つまり内部的には別実装でも、外から見ると

- MySQL クライアント/ORM から接続できる
- MySQL っぽい SQL が通る
- migration がかなりそのまま動く
- テストで欲しい transaction / schema / reset が速い

というものを目指す。

---

## 何をゴールにするか

まずスコープを厳密に切るべきです。

### 目標

- テストコードからは MySQL として扱える
- mysql2, go-sql-driver/mysql, JDBC MySQL driver あたりから接続できる
- 主要な DDL / DML が動く
- 1プロセスで軽量に起動できる
- file-based / in-memory 両対応
- DB作成、破棄、reset が超速い
- migration を高確率でそのまま流せる

### 非目標

- 本番トラフィックを捌く
- MySQL optimizer 完全互換
- 分散、レプリケーション、GTID
- InnoDB 完全再現
- 実運用の durability
- 完全 SQL 標準準拠
- 全 MySQL 機能の実装

ここを曖昧にすると破綻します。

---

## いちばん大事な判断

### 1. wire protocol 互換をやるか

ここはかなり重要です。

#### A. MySQL wire protocol 対応する

メリット:
- ORM/ドライバ差し替え不要
- 本当に「MySQLっぽく」使える
- テスト導入が楽

デメリット:
- 初期実装コストが高い
- handshake / prepared statement / auth / resultset framing が必要

#### B. 独自APIだけにする

メリット:
- 実装が圧倒的に楽
- 中身に集中できる

デメリット:
- 既存ORMやmigration toolをそのまま使えない
- 結局ラッパーが大量に必要

#### 推奨

**最初から wire protocol 対応をやるべき** です。
理由は、あなたの痛みが「SQLiteとの差分」なので、接続方法まで変えると意味が薄れるからです。

ただし最初は

- TCP
- 平文
- 認証は固定 or no-auth
- 単一接続でもよい

くらいに絞って始めるべきです。

---

## 全体アーキテクチャ

おすすめは5層です。

### 1. Protocol Layer

役割:
- MySQL handshake
- query packet 処理
- prepared statement API
- result set / OK / ERR packet 返却

ここは「MySQL クライアントから見た顔」です。

---

### 2. SQL Frontend Layer

役割:
- SQL parser
- AST 化
- MySQL 方言の吸収
- semantic analysis

ここでは MySQL の文法を受けます。

大事なのは、内部エンジンに MySQL 文法を直接持ち込まず、
MySQL AST → internal IR に変換することです。

---

### 3. Planning / Rewrite Layer

役割:
- MySQL固有構文を内部表現に落とす
- 型解決
- 名前解決
- AUTO_INCREMENT, ON DUPLICATE KEY, LIMIT, UNSIGNED, backtick identifiers などの正規化

ここが互換性の中心になります。

---

### 4. Execution Engine

役割:
- table scan
- index lookup
- insert/update/delete
- transaction
- MVCC or simplified snapshot
- join / filter / sort / aggregate

テスト用なら、ここは本気の optimizer は不要です。
まずは単純実装でよいです。

---

### 5. Storage Engine

役割:
- in-memory storage
- file-backed storage
- catalog/schema metadata
- WAL or snapshot
- fast reset / clone

SQLite 的な使い勝手を出すなら、ここはかなり重要です。

---

## おすすめの最初の実装戦略

戦略: 「まずは parser 互換 + execution 簡易」

最初から難しいところに行かず、こう切るのがよいです。

### Phase 1

- 単一プロセス
- 単一ノード
- 単一DB or 複数DB名だけサポート
- row-store
- in-memory メイン
- 基本DDL/DML
- 単純な index
- transaction は最初は簡易でもよい

### Phase 2

- file persistence
- prepared statement
- schema clone / snapshot
- migration friendliness 強化

### Phase 3

- JSON
- INFORMATION_SCHEMA 互換
- collation/charset の一部
- より多くの MySQL builtin functions

---

## 互換性の優先順位

全部を同時にやると死ぬので、優先順位をつけるべきです。

### Tier 1: 最優先

これはないと「SQLite よりマシ」になりにくいです。

- CREATE TABLE
- DROP TABLE
- ALTER TABLE の主要パターン
- CREATE INDEX
- INSERT
- UPDATE
- DELETE
- SELECT
- WHERE
- ORDER BY
- GROUP BY
- LIMIT
- JOIN
- PRIMARY KEY
- UNIQUE
- NULL
- DEFAULT
- AUTO_INCREMENT
- BEGIN/COMMIT/ROLLBACK
- prepared statements
- SHOW TABLES
- DESCRIBE
- backticks
- `?` プレースホルダ

### Tier 2: 実務上かなり大事

- INSERT ... ON DUPLICATE KEY UPDATE
- TEXT, BLOB, DATETIME, TIMESTAMP
- BOOLEAN alias
- ENUM の簡易サポート
- IF NOT EXISTS
- CREATE DATABASE / USE
- information_schema の最低限
- foreign key の最低限
- COUNT, SUM, MAX, MIN
- CTE (WITH)
- basic window functions

### Tier 3: 後回しでよい

- optimizer hints
- stored procedures
- triggers
- views 完全互換
- partitioning
- fulltext
- GIS
- collation 完全互換
- GTID/replication

---

## いちばん大事な設計判断: 内部表現

### SQL AST と内部IRを分ける

これは必須です。

悪い設計:
> parser の AST をそのまま実行器に食わせる

良い設計:
> MySQL AST → semantic analyzed AST → logical plan → physical plan

こう分けることです。

おすすめは最低でも以下:

1. Parser AST
2. Resolved IR
3. Execution Plan

これを分けると

- MySQL 方言を吸収しやすい
- エラーメッセージ位置を維持できる
- 後で optimizer を差し込みやすい
- 別 dialect 追加も可能

---

## ストレージ設計

テスト用なら、ストレージはかなり割り切れます。

### 推奨

row-store + append-friendly + snapshot clone 重視

### 必要な性質

- 起動が速い
- schema 作成が速い
- DB全体 reset が速い
- テストごとの isolation が簡単
- in-memory と file-backed を切り替えやすい

### 具体案

#### 方式A: pure in-memory + snapshot dump

- 全テーブルをメモリに保持
- テスト開始前の schema 済み状態を snapshot
- 各テストで snapshot clone

これはかなり強いです。

#### 方式B: copy-on-write page store

- 基本ページを共有
- 書き込み時だけ差分
- rollback/reset が速い

最初は A でいいです。
B は後で性能が必要になったら。

---

## reset を最優先機能にする

本番DBでは reset は補助ですが、テストDBでは主役です。

なので API と内部設計を

- create empty db
- apply migrations
- freeze snapshot
- fork from snapshot
- reset to snapshot

中心で設計した方がよいです。

SQLite っぽい体験を出すなら、ここが最重要です。

---

## transaction 設計

ここは悩みどころですが、テスト用なら段階的でよいです。

### Phase 1

- 単純な transaction
- table-level or coarse lock
- rollback journal 方式

### Phase 2

- statement atomicity
- row versioning
- snapshot read

### Phase 3

- MySQLに近い isolation レベル
- REPEATABLE READ 近似
- SELECT ... FOR UPDATE

最初から InnoDB 風 MVCC 完全再現は無理です。
テストDBとして重要なのは、まず

- rollback できる
- transaction 境界が守られる
- 基本競合で壊れない

です。

---

## DDL の扱い

MySQL 互換を感じるのは DDL 差分が大きいです。
なので DML より DDL をかなり頑張る価値があります。

### 特に重要

- AUTO_INCREMENT
- UNSIGNED
- ENGINE=InnoDB は受理して無視でも可
- CHARSET=utf8mb4 は受理して一旦 metadata のみでも可
- COLLATE= はまず受理して限定対応
- COMMENT は保存だけでもよい
- ALTER TABLE ADD COLUMN
- ALTER TABLE DROP COLUMN
- ALTER TABLE ADD INDEX
- ALTER TABLE MODIFY COLUMN

**"意味は完全再現しなくても、migration が通る" はかなり価値があります。**

---

## 型システム

ここは SQLite と大きく違うので重要です。

### おすすめ方針

MySQL 互換の論理型を持ち、内部実装型とは分ける

例:

> INT, BIGINT, VARCHAR(n), TEXT, DOUBLE, DECIMAL, DATE, DATETIME, TIMESTAMP, JSON, BLOB, BOOLEAN

内部的にはもっと単純でもいいですが、 外から見える型と validation は MySQL 寄りにした方がよいです。

特に大事なのは

- NULL 制約
- DEFAULT
- 桁あふれ
- 文字列長
- AUTO_INCREMENT
- UNSIGNED

です。

---

## MySQL らしさを出す小技

これをやると「かなり互換っぽく」なります。

- backtick quoted identifiers
- SHOW TABLES
- SHOW COLUMNS FROM x
- DESCRIBE x
- LAST_INSERT_ID()
- @@version
- SELECT DATABASE()
- USE dbname
- SET sql_mode=... を受理して一部だけ反映 or 無視
- ENGINE=InnoDB を受理
- utf8mb4 を受理

**実害の少ない構文は 受理して無害化 するのが大事です。**

---

## parser は自作しない方がいい

ここはかなり強くおすすめです。

### 方針

parser は既存資産を使う

自分で作るのは semantic / rewrite / execution

### 理由:

- SQL parser は意外と沼
- MySQL 方言は細かい
- エラーメッセージや precedence で消耗する

**自作したくなるけど、そこは差別化ポイントではないです。**

---

## 実装言語のおすすめ

### Rust

かなり向いています。

- 安定した単一バイナリ
- インメモリ/ファイル管理に向く
- protocol 実装しやすい
- 速度も十分
- あなたの志向にも合いそう

### Go

これも強いです。

- 並行処理しやすい
- server 実装が楽
- 開発速度が高い

### 個人的推奨

- 中長期で綺麗に作るなら Rust
- 早く PoC を出すなら Go

---

## 実装の切り方

### MVP

まずはここまで。

#### 接続

- MySQL wire protocol の最小実装
- username/password は固定でも可
- text protocol query 実行

#### DDL

- CREATE TABLE
- DROP TABLE
- ALTER TABLE 最低限
- CREATE INDEX

#### DML

- INSERT
- SELECT
- UPDATE
- DELETE
- ON DUPLICATE KEY

#### catalog

- DB / table / column metadata
- SHOW TABLES
- DESCRIBE

#### transaction

- BEGIN/COMMIT/ROLLBACK

#### storage

- in-memory
- snapshot/reset

#### テスト用機能

- CREATE SNAPSHOT
- RESET TO SNAPSHOT
- CLONE DATABASE

この最後の3つは MySQL 互換外でも、独自管理APIとして入れる価値があります。

---

## API設計

ユーザー向けには2面あるとよいです。

### 1. MySQL 互換接続面

- 通常の MySQL DSN で接続
- 既存ORM/migration toolが使える

### 2. テスト制御API

- 管理用 HTTP or local socket
- reset
- snapshot
- clone
- seed
- shutdown

SQLだけで reset を表現しようとするとつらいので、管理APIは分けた方がいいです。

---

## SQLiteとの差分で消耗しないための最重要戦略

ここが本質です。

**互換を「実装する」のではなく、「吸収する」設計にする。**

つまり、

- MySQL の migration が来る
- unsupported を全部即エラーにしない
- 受理できるものは受理して metadata 化
- 実動作に必要なものから真面目に実装
- 無視できるものは warning にする

という姿勢です。

たとえば

- ENGINE=InnoDB
- ROW_FORMAT=DYNAMIC
- CHARSET=utf8mb4
- COLLATE=utf8mb4_0900_ai_ci

みたいなものは、最初は完全再現できなくても **"DDL が落ちない" だけで体験は相当良くなります。**

---

## 開発ロードマップ

### Phase 0: 仕様固定

- 互換目標を決める
- サポートSQL一覧
- 非サポート明記
- MySQLのどの挙動を真似るか決める

### Phase 1: skeleton

- protocol 最小実装
- parser 接続
- catalog
- CREATE TABLE, INSERT, SELECT

### Phase 2: test usable

- migration がある程度通る
- transaction
- index
- reset/snapshot
- SHOW 系

### Phase 3: real-world useful

- ALTER TABLE 強化
- ON DUPLICATE KEY
- prepared statement
- LAST_INSERT_ID
- info schema 最低限

### Phase 4: polish

- JSON
- CTE
- more functions
- collation subset
- better errors

---

## テスト戦略

自分が作るDBのテストは3種類必要です。

### 1. sqllogic 的テスト

- SQL入力
- 結果表
- エラー期待

### 2. MySQL differential test

- 同じ SQL を MySQL 本物にも流す
- 結果とエラーを比較

**これが最重要です。**

### 3. migration corpus test

- 実際の OSS / 社内 migration セットを流す
- どこで落ちるか集計

この3つがあるとかなり強いです。

---

## いちばんおすすめの割り切り

これをやると現実的になります。

### 割り切り案

- optimizer は雑でいい
- 性能より互換性優先
- durability は弱くてよい
- concurrency も最初は弱くてよい
- DDL compatibility をかなり優先
- reset/snapshot を最優先で最適化

**テストDBとしての価値は QPS ではなく、導入の楽さと本番差分の少なさ です。**

---

## まとめると、提案したい設計はこれです

### 提案

**「MySQL wire-compatible, test-first, snapshot-oriented embedded DB server」**

### キーアイデア

- MySQL クライアント互換
- テスト専用に割り切る
- in-memory / file-backed 両対応
- DDL互換を重視
- reset/snapshot/fork を最適化
- optimizer や durability は後回し

### 優先順位

1. wire protocol
2. migration が通る DDL
3. 基本 DML
4. snapshot/reset
5. prepared statements
6. 実務でよく使う MySQL 関数
7. JSON / CTE / window function

---

必要なら次に、 このDBの spec.md のたたき台 として

- システム構成
- モジュール分割
- SQLサポート表
- ストレージ設計
- transaction model
- protocol scope
- MVP milestones

まで、実際に書けます。
