# PostgreSQL地理空間クエリツール群

PostgreSQLデータベースに接続して地理空間情報の統計処理を行うツール群です。LLMとの統合により、自然言語からSQLクエリを自動生成し、地理空間データの分析を効率化します。

## ツール一覧

### 1. `postgres_spatial_query.py`
基本的なPostgreSQL地理空間クエリツール

**主要機能:**
- 地理的範囲でのフィルタリング（bbox、point、polygon）
- 建物属性の統計処理
- 災害リスク情報の集計
- LLMによるクエリ生成と実行

**使用例:**
```python
from postgres_spatial_query import PostgresSpatialQuery

tool = PostgresSpatialQuery()
result = tool.run(json.dumps({
    "user_prompt": "大阪市内の建物用途別件数を集計して",
    "spatial_filter": {
        "type": "bbox",
        "coordinates": [135.4, 34.6, 135.6, 34.8]
    },
    "max_rows": 50
}))
```

### 2. `llm_query_generator.py`
LLM統合クエリ生成ツール

**主要機能:**
- 自然言語からSQLクエリの自動生成
- 地理空間条件の自動解釈
- クエリの安全性チェック
- エラー時の自動修正

**使用例:**
```python
from llm_query_generator import LLMQueryGenerator, NaturalLanguageResponse

# クエリ生成
query_gen = LLMQueryGenerator()
result = query_gen.run(json.dumps({
    "user_prompt": "建物の高さ別分布を分析して",
    "context": "大阪市内の建物データ",
    "max_rows": 100
}))

# 自然言語応答生成
response_gen = NaturalLanguageResponse()
answer = response_gen.run(json.dumps({
    "user_prompt": "建物の高さ別分布を分析して",
    "result": json.loads(result)["result"],
    "style": "summary"
}))
```

### 3. `secure_postgres_tool.py`
セキュアPostgreSQLツール統合版

**主要機能:**
- 包括的なエラーハンドリング
- SQLインジェクション対策
- 接続プール管理
- ログ記録
- パフォーマンス監視

**使用例:**
```python
from secure_postgres_tool import SecurePostgresTool

tool = SecurePostgresTool()
result = tool.run(json.dumps({
    "user_prompt": "災害リスクの高い建物を用途別に集計して",
    "context": "大阪市内の災害リスク分析",
    "max_rows": 100,
    "retries": 2
}))
```

## 環境設定

### 必要な環境変数
```bash
# PostgreSQL接続情報
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=plateau
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# OpenAI API（LLM機能用）
OPENAI_API_KEY=your_api_key_here
# または
OPEN_AI_API_KEY=your_api_key_here
```

### 依存関係のインストール
```bash
pip install psycopg2-binary langchain-core langchain-community langchain-openai python-dotenv
```

## データベーススキーマ

### buildingsテーブル
建物の基本情報と地理空間データ
- `id`: 建物ID
- `name`: 建物名
- `class`: 建物クラス
- `measured_height`: 測定高さ
- `detailed_usage`: 詳細用途
- `geometry`: 地理空間情報（EPSG:4979座標系）
- 災害リスク集計情報（disaster_risk_count等）

### disaster_risksテーブル
建物の災害リスク情報（正規化）
- `building_id`: 建物ID（外部キー）
- `risk_type`: リスクタイプ
- `disaster_category`: 災害カテゴリ
- `depth`: 浸水深
- `duration`: 浸水継続時間

## 使用パターン

### 1. 基本的な統計処理
```python
# 用途別建物数集計
result = tool.run(json.dumps({
    "user_prompt": "建物用途別の件数を集計して",
    "max_rows": 20
}))
```

### 2. 地理的範囲指定
```python
# 特定エリアの分析
result = tool.run(json.dumps({
    "user_prompt": "このエリアの建物高さ分布を分析して",
    "spatial_filter": {
        "type": "bbox",
        "coordinates": [135.4, 34.6, 135.6, 34.8]  # 経度・緯度の範囲
    }
}))
```

### 3. 災害リスク分析
```python
# 災害リスクの高い建物分析
result = tool.run(json.dumps({
    "user_prompt": "災害リスクの高い建物を用途別に集計して",
    "max_rows": 50
}))
```

### 4. 高さ別分析
```python
# 建物高さの分布分析
result = tool.run(json.dumps({
    "user_prompt": "建物の高さ別分布を分析して、カテゴリ別に集計して",
    "max_rows": 10
}))
```

## セキュリティ機能

### SQLインジェクション対策
- 禁止キーワードの検出
- 禁止関数の検出
- 入力サニタイズ
- SELECT文のみ許可

### パフォーマンス制限
- クエリ実行時間制限（デフォルト30秒）
- 結果行数制限（デフォルト10,000行）
- 接続プール管理

### ログ記録
- クエリ実行時間の記録
- エラーログの記録
- セキュリティ警告の記録

## エラーハンドリング

### 自動修正機能
- SQLエラー時の自動修正
- リトライ機能（デフォルト2回）
- エラー履歴の記録

### エラー例と対処法
```python
# エラー時の自動修正
result = tool.run(json.dumps({
    "user_prompt": "建物の用途別集計",
    "retries": 3  # 最大3回までリトライ
}))
```

## パフォーマンス最適化

### インデックス活用
- 地理空間インデックス（GIST）
- 建物IDインデックス
- 災害カテゴリインデックス

### 接続プール
- 接続の再利用
- 同時接続数制限
- 自動接続管理

## トラブルシューティング

### よくある問題

1. **接続エラー**
   - 環境変数の確認
   - PostgreSQLサービスの起動確認
   - ネットワーク接続の確認

2. **LLMエラー**
   - APIキーの設定確認
   - ネットワーク接続の確認
   - レート制限の確認

3. **SQLエラー**
   - スキーマの確認
   - 列名の確認
   - 座標系の確認

### デバッグ方法
```python
# ログレベルをDEBUGに設定
import logging
logging.basicConfig(level=logging.DEBUG)

# 詳細なエラー情報を取得
result = tool.run(json.dumps({
    "user_prompt": "your_query",
    "retries": 1
}))
print(json.loads(result))
```

## ライセンス

このツール群はMITライセンスの下で提供されています。
