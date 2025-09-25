#!/usr/bin/env python3
"""
27100_*GeoJSONファイルをPostgreSQLデータベースにインポートするスクリプト
大阪市の都市計画データ（道路、公園、用途地域など）をデータベースに格納
"""

import json
import psycopg2
import psycopg2.extras
from typing import Dict, List, Any, Optional, Tuple
import logging
import sys
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv
import glob

# 環境変数読み込み
load_dotenv(override=True)

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('27100_geojson_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class GeoJSON27100Importer:
    """27100_*GeoJSONファイル用のインポーター"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extras.RealDictCursor] = None
        
    def connect(self) -> bool:
        """データベース接続"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.connection.autocommit = False
            logger.info("データベース接続成功")
            return True
        except Exception as e:
            logger.error(f"データベース接続エラー: {e}")
            return False
    
    def disconnect(self) -> None:
        """データベース接続を閉じる"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("データベース接続を閉じました")
    
    def create_tables(self) -> bool:
        """27100データ用のテーブルを作成"""
        try:
            # PostGIS拡張機能の有効化
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            
            # 汎用都市計画データテーブル
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS urban_planning_data (
                id SERIAL PRIMARY KEY,
                data_type TEXT NOT NULL,
                feature_id TEXT,
                properties JSONB,
                geometry GEOMETRY(GEOMETRY, 6668), -- EPSG:6668 (JGD2011)
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.cursor.execute(create_table_sql)
            
            # インデックスの作成
            index_sqls = [
                "CREATE INDEX IF NOT EXISTS idx_urban_planning_data_type ON urban_planning_data(data_type);",
                "CREATE INDEX IF NOT EXISTS idx_urban_planning_data_geometry ON urban_planning_data USING GIST (geometry);",
                "CREATE INDEX IF NOT EXISTS idx_urban_planning_data_properties ON urban_planning_data USING GIN (properties);"
            ]
            
            for sql in index_sqls:
                self.cursor.execute(sql)
            
            self.connection.commit()
            logger.info("テーブル作成完了")
            return True
        except Exception as e:
            logger.error(f"テーブル作成エラー: {e}")
            self.connection.rollback()
            return False
    
    def get_data_type_from_filename(self, filename: str) -> str:
        """ファイル名からデータタイプを取得"""
        filename_lower = filename.lower()
        
        if 'bouka' in filename_lower:
            return '防火'
        elif 'chikukei' in filename_lower:
            return '地形'
        elif 'douro' in filename_lower:
            return '道路'
        elif 'fuuchichiku' in filename_lower:
            return '風致地区'
        elif 'koudori' in filename_lower or 'koudoti' in filename_lower:
            return '高度地区'
        elif 'kouen' in filename_lower:
            return '公園'
        elif 'senbiki' in filename_lower:
            return '線引き'
        elif 'tkbt' in filename_lower:
            return '建蔽率'
        elif 'tochiku' in filename_lower:
            return '土地区画'
        elif 'tokei' in filename_lower:
            return '統計'
        elif 'toshisaisei' in filename_lower:
            return '都市再生'
        elif 'youto' in filename_lower:
            return '用途'
        else:
            return 'その他'
    
    def create_geometry_sql(self, geometry: Dict[str, Any]) -> str:
        """ジオメトリオブジェクトからPostGISのSQLを作成"""
        if not geometry or 'type' not in geometry:
            return "NULL"
        
        geom_type = geometry.get('type')
        coordinates = geometry.get('coordinates')
        
        if not coordinates:
            return "NULL"
        
        try:
            if geom_type == 'Point':
                if len(coordinates) >= 2:
                    return f"ST_GeomFromText('POINT({coordinates[0]} {coordinates[1]})', 6668)"
            elif geom_type == 'LineString':
                coords_str = ', '.join([f"{coord[0]} {coord[1]}" for coord in coordinates])
                return f"ST_GeomFromText('LINESTRING({coords_str})', 6668)"
            elif geom_type == 'Polygon':
                # 外側のリングのみ処理
                outer_ring = coordinates[0]
                coords_str = ', '.join([f"{coord[0]} {coord[1]}" for coord in outer_ring])
                return f"ST_GeomFromText('POLYGON(({coords_str}))', 6668)"
            elif geom_type == 'MultiPolygon':
                # 最初のポリゴンのみ処理
                first_polygon = coordinates[0]
                if first_polygon:
                    outer_ring = first_polygon[0]
                    coords_str = ', '.join([f"{coord[0]} {coord[1]}" for coord in outer_ring])
                    return f"ST_GeomFromText('POLYGON(({coords_str}))', 6668)"
        except Exception as e:
            logger.warning(f"ジオメトリ変換エラー: {e}")
            return "NULL"
        
        return "NULL"
    
    def import_geojson_file(self, file_path: str, data_type: str, batch_size: int = 1000) -> bool:
        """単一のGeoJSONファイルをインポート"""
        try:
            logger.info(f"ファイル読み込み開始: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
            
            features = geojson_data.get('features', [])
            total_features = len(features)
            logger.info(f"総フィーチャー数: {total_features}")
            
            success_count = 0
            error_count = 0
            
            for i, feature in enumerate(features):
                try:
                    # データ抽出
                    properties = feature.get('properties', {})
                    geometry = feature.get('geometry', {})
                    
                    # フィーチャーIDを生成
                    feature_id = properties.get('id') or f"{data_type}_{i}"
                    
                    # ジオメトリSQL作成
                    geometry_sql = self.create_geometry_sql(geometry)
                    
                    # データ挿入
                    insert_sql = """
                        INSERT INTO urban_planning_data (
                            data_type, feature_id, properties, geometry
                        ) VALUES (
                            %s, %s, %s, %s
                        )
                    """
                    
                    if geometry_sql != "NULL":
                        # ジオメトリSQLを直接実行
                        self.cursor.execute(
                            insert_sql.replace('%s', '%s, %s, %s, ' + geometry_sql),
                            (data_type, feature_id, json.dumps(properties, ensure_ascii=False))
                        )
                    else:
                        # ジオメトリなしで挿入
                        self.cursor.execute(
                            insert_sql.replace('%s', '%s, %s, %s, NULL'),
                            (data_type, feature_id, json.dumps(properties, ensure_ascii=False))
                        )
                    
                    success_count += 1
                    
                    # バッチコミット
                    if (i + 1) % batch_size == 0:
                        self.connection.commit()
                        logger.info(f"進捗: {i + 1}/{total_features} ({((i + 1) / total_features) * 100:.1f}%)")
                
                except Exception as e:
                    logger.error(f"フィーチャー {i} 処理エラー: {e}")
                    error_count += 1
                    continue
            
            # 最終コミット
            self.connection.commit()
            
            logger.info(f"インポート完了 - 成功: {success_count}, エラー: {error_count}")
            return error_count == 0
            
        except Exception as e:
            logger.error(f"GeoJSONインポートエラー: {e}")
            return False
    
    def import_all_27100_files(self, directory: str, batch_size: int = 1000) -> bool:
        """27100_*GeoJSONファイルをすべてインポート"""
        try:
            # 27100_*geojsonファイルを検索
            pattern = os.path.join(directory, "27100_*.geojson")
            geojson_files = glob.glob(pattern)
            
            if not geojson_files:
                logger.warning("27100_*.geojsonファイルが見つかりません")
                return False
            
            logger.info(f"見つかったファイル数: {len(geojson_files)}")
            
            total_success = 0
            total_errors = 0
            
            for file_path in geojson_files:
                filename = os.path.basename(file_path)
                data_type = self.get_data_type_from_filename(filename)
                
                logger.info(f"処理開始: {filename} (データタイプ: {data_type})")
                
                if self.import_geojson_file(file_path, data_type, batch_size):
                    total_success += 1
                    logger.info(f"成功: {filename}")
                else:
                    total_errors += 1
                    logger.error(f"失敗: {filename}")
            
            logger.info(f"全体結果 - 成功: {total_success}, 失敗: {total_errors}")
            return total_errors == 0
            
        except Exception as e:
            logger.error(f"全体インポートエラー: {e}")
            return False

def main():
    """メイン関数"""
    # データベース設定
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'plateau_buildings'),
        'user': os.getenv('DB_USER', 'h-hibara'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    # 27100_GeoJSONディレクトリのパス
    geojson_directory = os.path.dirname(os.path.abspath(__file__))
    batch_size = int(os.getenv('BATCH_SIZE', 1000))
    
    # インポーター初期化
    importer = GeoJSON27100Importer(db_config)
    
    try:
        # データベース接続
        if not importer.connect():
            logger.error("データベース接続に失敗しました")
            return False
        
        # テーブル作成
        if not importer.create_tables():
            logger.error("テーブル作成に失敗しました")
            return False
        
        # 全ファイルインポート
        start_time = datetime.now()
        if not importer.import_all_27100_files(geojson_directory, batch_size):
            logger.error("データインポートに失敗しました")
            return False
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"処理時間: {duration}")
        logger.info("すべての処理が完了しました")
        return True
        
    except Exception as e:
        logger.error(f"メイン処理エラー: {e}")
        return False
    
    finally:
        importer.disconnect()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
