-- PostgreSQLデータベーススキーマ設計
-- Building_optimized.geojson用のデータベース構造

-- PostGIS拡張機能の有効化
CREATE EXTENSION IF NOT EXISTS postgis;

-- メインテーブル: 建物情報
CREATE TABLE buildings (
    id TEXT PRIMARY KEY,
    name TEXT,
    class TEXT,
    measured_height DECIMAL(10,2),
    specified_building_coverage_rate DECIMAL(3,2),
    survey_year TEXT,
    building_type TEXT,
    building_roof_edge_area DECIMAL(15,2),
    building_structure_type TEXT,
    building_structure_org_type TEXT,
    detailed_usage TEXT,
    ground_floor_usage TEXT,
    second_floor_usage TEXT,
    third_floor_usage TEXT,
    basement_first_usage TEXT,
    basement_second_usage TEXT,
    -- 災害リスク集計情報
    disaster_risk_count INTEGER,
    disaster_category_river_flooding_count INTEGER,
    disaster_category_tsunami_count INTEGER,
    disaster_category_high_tide_count INTEGER,
    max_flood_depth DECIMAL(10,3),
    min_flood_depth DECIMAL(10,3),
    avg_flood_depth DECIMAL(10,3),
    -- 地理情報 (PostGIS geometry型)
    geometry GEOMETRY(POINT, 4979), -- EPSG:4979 (WGS84 3D)
    -- メタデータ
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 災害リスクテーブル (正規化)
CREATE TABLE disaster_risks (
    id SERIAL PRIMARY KEY,
    building_id TEXT NOT NULL REFERENCES buildings(id) ON DELETE CASCADE,
    risk_number INTEGER NOT NULL, -- 1, 2, 3
    risk_type TEXT,
    description TEXT,
    rank TEXT,
    depth DECIMAL(10,3),
    admin_type TEXT,
    scale TEXT,
    duration DECIMAL(10,2),
    disaster_category TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- インデックスの作成
-- 地理空間インデックス
CREATE INDEX idx_buildings_geometry ON buildings USING GIST (geometry);

-- 建物IDインデックス
CREATE INDEX idx_buildings_id ON buildings(id);
CREATE INDEX idx_disaster_risks_building_id ON disaster_risks(building_id);

-- 災害カテゴリインデックス
CREATE INDEX idx_disaster_risks_category ON disaster_risks(disaster_category);
CREATE INDEX idx_disaster_risks_type ON disaster_risks(risk_type);

-- 建物クラスインデックス
CREATE INDEX idx_buildings_class ON buildings(class);

-- 災害リスク数インデックス
CREATE INDEX idx_buildings_disaster_count ON buildings(disaster_risk_count);

-- 地理的範囲検索用のインデックス
CREATE INDEX idx_buildings_geometry_2d ON buildings USING GIST (ST_Transform(geometry, 4326));

-- コメントの追加
COMMENT ON TABLE buildings IS '建物の基本情報と地理空間データ';
COMMENT ON TABLE disaster_risks IS '建物の災害リスク情報（正規化）';
COMMENT ON COLUMN buildings.geometry IS '建物の位置情報（EPSG:4979座標系）';
COMMENT ON COLUMN disaster_risks.risk_number IS '災害リスクの番号（1, 2, 3）';
COMMENT ON COLUMN disaster_risks.depth IS '浸水深（メートル）';
COMMENT ON COLUMN disaster_risks.duration IS '浸水継続時間（時間）';
