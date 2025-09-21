# Building_optimized.geojson の PostgreSQL データベース化

このドキュメントでは、`Building_optimized.geojson` ファイルを PostgreSQL データベースに格納する手順を説明します。

## 前提条件

- PostgreSQL 12以上
- PostGIS 3.0以上
- Python 3.8以上
- 十分なディスク容量（約2GB以上推奨）

## 1. PostgreSQL と PostGIS のインストール

### macOS (Homebrew)
```bash
# PostgreSQL のインストール
brew install postgresql postgis

# PostgreSQL の起動
brew services start postgresql

# PostGIS 拡張機能のインストール
brew install postgis
```

### Ubuntu/Debian
```bash
# PostgreSQL と PostGIS のインストール
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib postgis postgresql-12-postgis-3

# PostgreSQL の起動
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### Windows
1. [PostgreSQL 公式サイト](https://www.postgresql.org/download/windows/) からインストーラーをダウンロード
2. インストール時に PostGIS 拡張機能も選択

## 2. データベースの作成

```bash
# PostgreSQL に接続
psql -U postgres

# データベース作成
CREATE DATABASE plateau_buildings;

# データベースに接続
\c plateau_buildings

# PostGIS 拡張機能の有効化
CREATE EXTENSION postgis;

# 接続確認
SELECT PostGIS_Version();
```

## 3. Python 環境のセットアップ

```bash
# 必要なパッケージのインストール
pip install -r requirements.txt

# 環境変数ファイルの作成
cp env_example.txt .env
# .env ファイルを編集してデータベース接続情報を設定
```

## 4. データベーススキーマの作成

```bash
# スキーマファイルを実行
psql -U postgres -d plateau_buildings -f database_schema.sql
```

## 5. データのインポート

### 基本インポート（メモリ使用量が多い）
```bash
python geojson_to_postgresql.py
```

### 最適化インポート（推奨）
```bash
python optimized_geojson_importer.py
```

## 6. インポート結果の確認

```sql
-- データベースに接続
psql -U postgres -d plateau_buildings

-- テーブル一覧の確認
\dt

-- 建物データの件数確認
SELECT COUNT(*) FROM buildings;

-- 災害リスクデータの件数確認
SELECT COUNT(*) FROM disaster_risks;

-- サンプルデータの確認
SELECT id, class, measured_height, ST_AsText(geometry) as location 
FROM buildings 
LIMIT 5;

-- 災害リスクの集計
SELECT disaster_category, COUNT(*) as count
FROM disaster_risks 
WHERE disaster_category IS NOT NULL
GROUP BY disaster_category;
```

## 7. 地理空間クエリの例

```sql
-- 特定の座標範囲内の建物を検索
SELECT id, class, measured_height
FROM buildings 
WHERE ST_Contains(
    ST_MakeEnvelope(135.4, 34.6, 135.5, 34.7, 4979),
    geometry
);

-- 特定の建物から半径1km以内の建物を検索
SELECT b2.id, b2.class, ST_Distance(b1.geometry, b2.geometry) as distance
FROM buildings b1, buildings b2
WHERE b1.id = 'bldg_7effc46c-d994-4243-8874-a553ec08a7b5'
AND b1.id != b2.id
AND ST_DWithin(b1.geometry, b2.geometry, 1000)
ORDER BY distance;

-- 災害リスクの高い建物を検索
SELECT b.id, b.class, dr.depth, dr.disaster_category
FROM buildings b
JOIN disaster_risks dr ON b.id = dr.building_id
WHERE dr.depth > 3.0
ORDER BY dr.depth DESC;
```

## 8. パフォーマンス最適化

### インデックスの確認
```sql
-- インデックスの一覧
\di

-- インデックス使用状況の確認
EXPLAIN ANALYZE SELECT * FROM buildings WHERE class = '普通建物';
```

### 統計情報の更新
```sql
-- 統計情報の更新
ANALYZE buildings;
ANALYZE disaster_risks;
```

## 9. トラブルシューティング

### よくある問題と解決方法

1. **メモリ不足エラー**
   - バッチサイズを小さくする（BATCH_SIZE=500）
   - システムのメモリを増やす

2. **接続エラー**
   - データベース設定を確認
   - PostgreSQL が起動しているか確認

3. **権限エラー**
   - データベースユーザーの権限を確認
   - `GRANT ALL PRIVILEGES ON DATABASE plateau_buildings TO your_user;`

4. **PostGIS エラー**
   - PostGIS 拡張機能が正しくインストールされているか確認
   - `SELECT PostGIS_Version();` で確認

## 10. データの活用例

### Web アプリケーションでの利用
```python
import psycopg2
import json

# データベース接続
conn = psycopg2.connect(
    host='localhost',
    database='plateau_buildings',
    user='postgres',
    password='your_password'
)

# 地理空間クエリ
cursor = conn.cursor()
cursor.execute("""
    SELECT id, class, ST_AsGeoJSON(geometry) as geometry
    FROM buildings 
    WHERE ST_Contains(
        ST_MakeEnvelope(%s, %s, %s, %s, 4979),
        geometry
    )
""", (min_lon, min_lat, max_lon, max_lat))

results = cursor.fetchall()
```

### API での利用
```python
from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

@app.route('/api/buildings/<building_id>')
def get_building(building_id):
    # 建物情報と災害リスクを取得
    # 実装...
    pass
```

## まとめ

このセットアップにより、約61万件の建物データを効率的にPostgreSQLデータベースに格納し、地理空間クエリや災害リスク分析を行うことができます。PostGISの強力な地理空間機能を活用して、様々な分析やアプリケーション開発が可能になります。
