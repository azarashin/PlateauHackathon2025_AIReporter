#!/usr/bin/env python3
"""
最適化されたGeoJSONからPostgreSQLへのデータ移行スクリプト
大量データの効率的な処理とメモリ最適化を実装
"""

import json
import psycopg2
import psycopg2.extras
from typing import Dict, List, Any, Optional, Tuple, Generator
import logging
import sys
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geojson_import_optimized.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class OptimizedGeoJSONImporter:
    """最適化されたGeoJSONインポーター"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extras.RealDictCursor] = None
        
    def connect(self) -> bool:
        """データベース接続"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # 自動コミットを無効化（バッチ処理のため）
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
        """テーブル作成"""
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
    
    def stream_geojson_features(self, geojson_file: str) -> Generator[Dict[str, Any], None, None]:
        """
        GeoJSONファイルをストリーミング読み込み
        メモリ効率を向上させるため、一度に全データを読み込まない
        """
        try:
            with open(geojson_file, 'r', encoding='utf-8') as f:
                # ファイルの先頭部分を読み込んでfeaturesの開始位置を特定
                buffer = ""
                features_start = False
                
                while True:
                    chunk = f.read(8192)  # 8KBずつ読み込み
                    if not chunk:
                        break
                    
                    buffer += chunk
                    
                    # features配列の開始を検出
                    if not features_start and '"features":' in buffer:
                        features_start = True
                        # features配列の開始位置を特定
                        start_pos = buffer.find('"features": [') + len('"features": [')
                        buffer = buffer[start_pos:]
                        continue
                    
                    if features_start:
                        # 完全なJSONオブジェクトを抽出
                        while True:
                            # 次のfeatureの開始を探す
                            feature_start = buffer.find('{')
                            if feature_start == -1:
                                break
                            
                            # 対応する}を見つける
                            brace_count = 0
                            feature_end = -1
                            
                            for i, char in enumerate(buffer[feature_start:], feature_start):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        feature_end = i + 1
                                        break
                            
                            if feature_end != -1:
                                # 完全なfeatureを抽出
                                feature_json = buffer[feature_start:feature_end]
                                try:
                                    feature = json.loads(feature_json)
                                    yield feature
                                except json.JSONDecodeError:
                                    pass
                                
                                # 処理済み部分を削除
                                buffer = buffer[feature_end:]
                            else:
                                break
                
        except Exception as e:
            logger.error(f"GeoJSONストリーミングエラー: {e}")
            raise
    
    def prepare_building_data(self, feature: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """建物データの準備（型変換とバリデーション）"""
        props = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # 建物基本データ
        building_data = {
            'id': props.get('id'),
            'name': props.get('name'),
            'class': props.get('class'),
            'measured_height': self._safe_float(props.get('measuredHeight')),
            'specified_building_coverage_rate': self._safe_float(props.get('specifiedBuildingCoverageRate')),
            'survey_year': props.get('surveyYear'),
            'building_type': props.get('type'),
            'building_roof_edge_area': self._safe_float(props.get('buildingRoofEdgeArea')),
            'building_structure_type': props.get('buildingStructureType'),
            'building_structure_org_type': props.get('buildingStructureOrgType'),
            'detailed_usage': props.get('detailedUsage'),
            'ground_floor_usage': props.get('groundFloorUsage'),
            'second_floor_usage': props.get('secondFloorUsage'),
            'third_floor_usage': props.get('thirdFloorUsage'),
            'basement_first_usage': props.get('basementFirstUsage'),
            'basement_second_usage': props.get('basementSecondUsage'),
            'disaster_risk_count': self._safe_int(props.get('disaster_risk_count')),
            'disaster_category_river_flooding_count': self._safe_int(props.get('disaster_category_河川氾濫_count')),
            'disaster_category_tsunami_count': self._safe_int(props.get('disaster_category_津波_count')),
            'disaster_category_high_tide_count': self._safe_int(props.get('disaster_category_高潮_count')),
            'max_flood_depth': self._safe_float(props.get('max_flood_depth')),
            'min_flood_depth': self._safe_float(props.get('min_flood_depth')),
            'avg_flood_depth': self._safe_float(props.get('avg_flood_depth')),
            'geometry': self._create_geometry_sql(geometry)
        }
        
        # 災害リスクデータの抽出
        disaster_risks = []
        for i in range(1, 4):
            risk_data = {
                'building_id': props.get('id'),
                'risk_number': i,
                'risk_type': props.get(f'disaster_risk_{i}_type'),
                'description': props.get(f'disaster_risk_{i}_description'),
                'rank': props.get(f'disaster_risk_{i}_rank'),
                'depth': self._safe_float(props.get(f'disaster_risk_{i}_depth')),
                'admin_type': props.get(f'disaster_risk_{i}_admin_type'),
                'scale': props.get(f'disaster_risk_{i}_scale'),
                'duration': self._safe_float(props.get(f'disaster_risk_{i}_duration')),
                'disaster_category': props.get(f'disaster_risk_{i}_disaster_category')
            }
            
            # 有効な災害リスクデータのみ追加
            if any(risk_data[key] is not None for key in ['risk_type', 'description', 'depth']):
                disaster_risks.append(risk_data)
        
        return building_data, disaster_risks
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全なfloat変換"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全なint変換"""
        if value is None or value == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _create_geometry_sql(self, geometry: Dict[str, Any]) -> str:
        """ジオメトリSQL作成"""
        if geometry.get('type') == 'Point' and 'coordinates' in geometry:
            coords = geometry['coordinates']
            if len(coords) >= 2:
                return f"ST_GeomFromText('POINT({coords[0]} {coords[1]})', 4979)"
        return "NULL"
    
    def batch_insert_buildings(self, buildings_batch: List[Dict[str, Any]]) -> bool:
        """建物データのバッチ挿入"""
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
            
            # ジオメトリを特別処理
            for building in buildings_batch:
                if building['geometry'] and building['geometry'] != 'NULL':
                    geometry_sql = building['geometry']
                    building_copy = building.copy()
                    del building_copy['geometry']
                    
                    # ジオメトリSQLを直接実行
                    self.cursor.execute(
                        insert_sql.replace('%(geometry)s', geometry_sql), 
                        building_copy
                    )
                else:
                    building_copy = building.copy()
                    del building_copy['geometry']
                    self.cursor.execute(
                        insert_sql.replace('%(geometry)s', 'NULL'), 
                        building_copy
                    )
            
            return True
        except Exception as e:
            logger.error(f"建物データバッチ挿入エラー: {e}")
            return False
    
    def batch_insert_disaster_risks(self, risks_batch: List[Dict[str, Any]]) -> bool:
        """災害リスクデータのバッチ挿入"""
        try:
            if not risks_batch:
                return True
                
            insert_sql = """
                INSERT INTO disaster_risks (
                    building_id, risk_number, risk_type, description, rank,
                    depth, admin_type, scale, duration, disaster_category
                ) VALUES (
                    %(building_id)s, %(risk_number)s, %(risk_type)s, %(description)s, %(rank)s,
                    %(depth)s, %(admin_type)s, %(scale)s, %(duration)s, %(disaster_category)s
                )
            """
            
            self.cursor.executemany(insert_sql, risks_batch)
            return True
        except Exception as e:
            logger.error(f"災害リスクデータバッチ挿入エラー: {e}")
            return False
    
    def import_geojson_optimized(self, geojson_file: str, batch_size: int = 1000) -> bool:
        """最適化されたGeoJSONインポート"""
        try:
            logger.info(f"最適化インポート開始: {geojson_file}")
            logger.info(f"バッチサイズ: {batch_size}")
            
            buildings_batch = []
            risks_batch = []
            processed_count = 0
            success_count = 0
            error_count = 0
            
            for feature in self.stream_geojson_features(geojson_file):
                try:
                    building_data, disaster_risks = self.prepare_building_data(feature)
                    
                    buildings_batch.append(building_data)
                    risks_batch.extend(disaster_risks)
                    
                    processed_count += 1
                    
                    # バッチサイズに達したら挿入
                    if len(buildings_batch) >= batch_size:
                        if self.batch_insert_buildings(buildings_batch):
                            self.batch_insert_disaster_risks(risks_batch)
                            self.connection.commit()
                            success_count += len(buildings_batch)
                            logger.info(f"バッチ処理完了: {processed_count} 件処理済み")
                        else:
                            error_count += len(buildings_batch)
                            self.connection.rollback()
                        
                        # バッチをリセット
                        buildings_batch = []
                        risks_batch = []
                
                except Exception as e:
                    logger.error(f"フィーチャー処理エラー: {e}")
                    error_count += 1
                    continue
            
            # 残りのデータを処理
            if buildings_batch:
                if self.batch_insert_buildings(buildings_batch):
                    self.batch_insert_disaster_risks(risks_batch)
                    self.connection.commit()
                    success_count += len(buildings_batch)
                else:
                    error_count += len(buildings_batch)
                    self.connection.rollback()
            
            logger.info(f"インポート完了 - 処理済み: {processed_count}, 成功: {success_count}, エラー: {error_count}")
            return error_count == 0
            
        except Exception as e:
            logger.error(f"最適化インポートエラー: {e}")
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
    
    # GeoJSONファイルパス
    geojson_file = 'Building_optimized.geojson'
    batch_size = int(os.getenv('BATCH_SIZE', 1000))
    
    # インポーター初期化
    importer = OptimizedGeoJSONImporter(db_config)
    
    try:
        # データベース接続
        if not importer.connect():
            logger.error("データベース接続に失敗しました")
            return False
        
        # テーブル作成（既に存在する場合はスキップ）
        try:
            importer.create_tables()
        except Exception as e:
            logger.info(f"テーブル作成をスキップ: {e}")
        
        # 最適化インポート実行
        start_time = datetime.now()
        if not importer.import_geojson_optimized(geojson_file, batch_size):
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
