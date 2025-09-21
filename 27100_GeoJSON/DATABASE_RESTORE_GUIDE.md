# PostgreSQL データベース復元ガイド

## 概要
このガイドでは、共有されたPostgreSQLデータベースダンプファイルを復元する方法を説明します。

## 前提条件
- PostgreSQL 14以上がインストールされていること
- PostGIS拡張機能が利用可能であること

## 復元手順

### 1. データベースの作成
```bash
# データベースを作成
createdb plateau_buildings

# PostGIS拡張機能を有効化
psql -d plateau_buildings -c "CREATE EXTENSION postgis;"
```

### 2. ダンプファイルの復元

#### 圧縮版の場合（推奨）
```bash
# 圧縮ファイルを解凍して復元
gunzip -c plateau_buildings_dump.sql.gz | psql -d plateau_buildings
```

#### 非圧縮版の場合
```bash
# 直接復元
psql -d plateau_buildings -f plateau_buildings_dump.sql
```

### 3. 復元の確認
```sql
-- データベースに接続
psql -d plateau_buildings

-- テーブル一覧の確認
\dt

-- 建物数の確認
SELECT COUNT(*) FROM buildings;

-- 災害リスク数の確認
SELECT COUNT(*) FROM disaster_risks;

-- サンプルデータの確認
SELECT id, class, measured_height FROM buildings LIMIT 5;
```

## データベース構造

### テーブル一覧
- `buildings`: 建物基本情報（616,319件）
- `disaster_risks`: 災害リスク情報（1,246,343件）

### 主要なカラム
**buildings テーブル**
- `id`: 建物ID（TEXT）
- `class`: 建物クラス（普通建物、堅ろう建物など）
- `measured_height`: 測定高さ
- `geometry`: 地理空間データ（PostGIS）

**disaster_risks テーブル**
- `building_id`: 建物ID（外部キー）
- `disaster_category`: 災害カテゴリ（河川氾濫、高潮、津波）
- `depth`: 浸水深
- `rank`: リスクランク

## 活用例

### 地理空間クエリ
```sql
-- 特定範囲内の建物を検索
SELECT * FROM buildings 
WHERE ST_Within(geometry, ST_MakeEnvelope(135.5, 34.6, 135.6, 34.7, 4326));

-- 浸水リスクの高い建物を検索
SELECT b.id, b.class, dr.depth, dr.disaster_category
FROM buildings b
JOIN disaster_risks dr ON b.id = dr.building_id
WHERE dr.depth > 1.0
ORDER BY dr.depth DESC;
```

### 統計分析
```sql
-- 建物クラス別の分布
SELECT class, COUNT(*) as count
FROM buildings 
GROUP BY class 
ORDER BY count DESC;

-- 災害リスク別の分布
SELECT disaster_category, COUNT(*) as count
FROM disaster_risks 
GROUP BY disaster_category 
ORDER BY count DESC;
```

## トラブルシューティング

### よくある問題
1. **PostGIS拡張機能エラー**
   ```sql
   CREATE EXTENSION IF NOT EXISTS postgis;
   ```

2. **権限エラー**
   ```bash
   # データベースユーザーの権限を確認
   psql -c "\du"
   ```

3. **メモリ不足**
   - 大きなダンプファイルの場合は、バッチサイズを調整
   - システムメモリを確認

## ファイルサイズ
- 非圧縮版: 434MB
- 圧縮版: 63MB（推奨）

## 注意事項
- 復元には十分なディスク容量が必要です
- 地理空間データのため、PostGIS拡張機能が必須です
- 復元時間はシステム性能により異なります
