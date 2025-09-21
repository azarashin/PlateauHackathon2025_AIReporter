#!/usr/bin/env python3
"""
GeoJSONからPostgreSQLへのデータ移行スクリプト
Building_optimized.geojsonをPostgreSQLデータベースにインポート
"""

import json
import psycopg2
import psycopg2.extras
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path
import sys
from datetime import datetime

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geojson_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class GeoJSONToPostgreSQLImporter:
    """GeoJSONからPostgreSQLへのデータ移行クラス"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        初期化
        
        Args:
            db_config: データベース接続設定
        """
        self.db_config = db_config
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extras.RealDictCursor] = None
        
    def connect(self) -> bool:
        """データベースに接続"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
        """テーブルを作成"""
        try:
            schema_file = Path(__file__).parent / "database_schema.sql"
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            self.cursor.execute(schema_sql)
            self.connection.commit()
            logger.info("テーブル作成完了")
            return True
        except Exception as e:
            logger.error(f"テーブル作成エラー: {e}")
            self.connection.rollback()
            return False
    
    def extract_building_data(self, feature: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        建物データと災害リスクデータを抽出
        
        Args:
            feature: GeoJSONのfeatureオブジェクト
            
        Returns:
            (building_data, disaster_risks_data)
        """
        props = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # 建物基本データ
        building_data = {
            'id': props.get('id'),
            'name': props.get('name'),
            'class': props.get('class'),
            'measured_height': props.get('measuredHeight'),
            'specified_building_coverage_rate': props.get('specifiedBuildingCoverageRate'),
            'survey_year': props.get('surveyYear'),
            'building_type': props.get('type'),
            'building_roof_edge_area': props.get('buildingRoofEdgeArea'),
            'building_structure_type': props.get('buildingStructureType'),
            'building_structure_org_type': props.get('buildingStructureOrgType'),
            'detailed_usage': props.get('detailedUsage'),
            'ground_floor_usage': props.get('groundFloorUsage'),
            'second_floor_usage': props.get('secondFloorUsage'),
            'third_floor_usage': props.get('thirdFloorUsage'),
            'basement_first_usage': props.get('basementFirstUsage'),
            'basement_second_usage': props.get('basementSecondUsage'),
            'disaster_risk_count': props.get('disaster_risk_count'),
            'disaster_category_river_flooding_count': props.get('disaster_category_河川氾濫_count'),
            'disaster_category_tsunami_count': props.get('disaster_category_津波_count'),
            'disaster_category_high_tide_count': props.get('disaster_category_高潮_count'),
            'max_flood_depth': props.get('max_flood_depth'),
            'min_flood_depth': props.get('min_flood_depth'),
            'avg_flood_depth': props.get('avg_flood_depth'),
            'geometry': self._create_geometry_sql(geometry)
        }
        
        # 災害リスクデータの抽出
        disaster_risks = []
        for i in range(1, 4):  # disaster_risk_1, disaster_risk_2, disaster_risk_3
            risk_data = {
                'building_id': props.get('id'),
                'risk_number': i,
                'risk_type': props.get(f'disaster_risk_{i}_type'),
                'description': props.get(f'disaster_risk_{i}_description'),
                'rank': props.get(f'disaster_risk_{i}_rank'),
                'depth': props.get(f'disaster_risk_{i}_depth'),
                'admin_type': props.get(f'disaster_risk_{i}_admin_type'),
                'scale': props.get(f'disaster_risk_{i}_scale'),
                'duration': props.get(f'disaster_risk_{i}_duration'),
                'disaster_category': props.get(f'disaster_risk_{i}_disaster_category')
            }
            
            # 有効な災害リスクデータのみ追加
            if any(risk_data[key] is not None for key in ['risk_type', 'description', 'depth']):
                disaster_risks.append(risk_data)
        
        return building_data, disaster_risks
    
    def _create_geometry_sql(self, geometry: Dict[str, Any]) -> str:
        """
        ジオメトリオブジェクトからPostGISのST_GeomFromGeoJSON関数用のSQLを作成
        
        Args:
            geometry: GeoJSONのgeometryオブジェクト
            
        Returns:
            PostGIS関数用のSQL文字列
        """
        if geometry.get('type') == 'Point' and 'coordinates' in geometry:
            coords = geometry['coordinates']
            if len(coords) >= 2:
                # EPSG:4979 (WGS84 3D) 座標系でPointを作成
                return f"ST_GeomFromText('POINT({coords[0]} {coords[1]})', 4979)"
        return "NULL"
    
    def insert_building(self, building_data: Dict[str, Any]) -> bool:
        """建物データを挿入"""
        try:
            insert_sql = """
                INSERT INTO buildings (
                    id, name, class, measured_height, specified_building_coverage_rate,
                    survey_year, building_type, building_roof_edge_area,
                    building_structure_type, building_structure_org_type,
                    detailed_usage, ground_floor_usage, second_floor_usage,
                    third_floor_usage, basement_first_usage, basement_second_usage,
                    disaster_risk_count, disaster_category_river_flooding_count,
                    disaster_category_tsunami_count, disaster_category_high_tide_count,
                    max_flood_depth, min_flood_depth, avg_flood_depth, geometry
                ) VALUES (
                    %(id)s, %(name)s, %(class)s, %(measured_height)s, %(specified_building_coverage_rate)s,
                    %(survey_year)s, %(building_type)s, %(building_roof_edge_area)s,
                    %(building_structure_type)s, %(building_structure_org_type)s,
                    %(detailed_usage)s, %(ground_floor_usage)s, %(second_floor_usage)s,
                    %(third_floor_usage)s, %(basement_first_usage)s, %(basement_second_usage)s,
                    %(disaster_risk_count)s, %(disaster_category_river_flooding_count)s,
                    %(disaster_category_tsunami_count)s, %(disaster_category_high_tide_count)s,
                    %(max_flood_depth)s, %(min_flood_depth)s, %(avg_flood_depth)s, %(geometry)s
                )
            """
            
            # geometryフィールドを特別に処理
            if building_data['geometry'] and building_data['geometry'] != 'NULL':
                # ジオメトリSQLを直接実行
                geometry_sql = building_data['geometry']
                building_data_copy = building_data.copy()
                del building_data_copy['geometry']
                
                # ジオメトリ以外のデータを挿入
                self.cursor.execute(insert_sql.replace('%(geometry)s', geometry_sql), building_data_copy)
            else:
                # ジオメトリなしで挿入
                building_data_copy = building_data.copy()
                del building_data_copy['geometry']
                self.cursor.execute(insert_sql.replace('%(geometry)s', 'NULL'), building_data_copy)
            
            return True
        except Exception as e:
            logger.error(f"建物データ挿入エラー: {e}")
            return False
    
    def insert_disaster_risks(self, disaster_risks: List[Dict[str, Any]]) -> bool:
        """災害リスクデータを挿入"""
        try:
            for risk in disaster_risks:
                insert_sql = """
                    INSERT INTO disaster_risks (
                        building_id, risk_number, risk_type, description, rank,
                        depth, admin_type, scale, duration, disaster_category
                    ) VALUES (
                        %(building_id)s, %(risk_number)s, %(risk_type)s, %(description)s, %(rank)s,
                        %(depth)s, %(admin_type)s, %(scale)s, %(duration)s, %(disaster_category)s
                    )
                """
                self.cursor.execute(insert_sql, risk)
            return True
        except Exception as e:
            logger.error(f"災害リスクデータ挿入エラー: {e}")
            return False
    
    def import_geojson(self, geojson_file: str, batch_size: int = 1000) -> bool:
        """
        GeoJSONファイルをインポート
        
        Args:
            geojson_file: GeoJSONファイルパス
            batch_size: バッチサイズ
        """
        try:
            logger.info(f"GeoJSONファイル読み込み開始: {geojson_file}")
            
            with open(geojson_file, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
            
            features = geojson_data.get('features', [])
            total_features = len(features)
            logger.info(f"総フィーチャー数: {total_features}")
            
            success_count = 0
            error_count = 0
            
            for i, feature in enumerate(features):
                try:
                    # データ抽出
                    building_data, disaster_risks = self.extract_building_data(feature)
                    
                    # 建物データ挿入
                    if self.insert_building(building_data):
                        # 災害リスクデータ挿入
                        if disaster_risks:
                            self.insert_disaster_risks(disaster_risks)
                        success_count += 1
                    else:
                        error_count += 1
                    
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

def main():
    """メイン関数"""
    # データベース設定（環境変数または設定ファイルから読み込み）
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'plateau_buildings',
        'user': 'postgres',
        'password': 'password'  # 実際のパスワードに変更
    }
    
    # GeoJSONファイルパス
    geojson_file = 'Building_optimized.geojson'
    
    # インポーター初期化
    importer = GeoJSONToPostgreSQLImporter(db_config)
    
    try:
        # データベース接続
        if not importer.connect():
            logger.error("データベース接続に失敗しました")
            return False
        
        # テーブル作成
        if not importer.create_tables():
            logger.error("テーブル作成に失敗しました")
            return False
        
        # データインポート
        if not importer.import_geojson(geojson_file):
            logger.error("データインポートに失敗しました")
            return False
        
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
