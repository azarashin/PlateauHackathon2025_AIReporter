#!/usr/bin/env python3
"""
インポート処理のテストスクリプト
小さなサンプルデータでインポート処理をテスト
"""

import json
import os
from pathlib import Path
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_geojson():
    """テスト用の小さなGeoJSONファイルを作成"""
    sample_data = {
        "type": "FeatureCollection",
        "name": "Building_sample_test",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4979"}},
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "class": "普通建物",
                    "measuredHeight": 10.5,
                    "id": "bldg_test_001",
                    "name": "テスト建物1",
                    "disaster_risk_1_type": "uro:RiverFloodingRiskAttribute",
                    "disaster_risk_1_description": "淀川水系淀川",
                    "disaster_risk_1_rank": "3m以上5m未満",
                    "disaster_risk_1_depth": 3.5,
                    "disaster_risk_1_admin_type": "国",
                    "disaster_risk_1_scale": "L2（想定最大規模）",
                    "disaster_risk_1_duration": 88.65,
                    "disaster_risk_1_disaster_category": "河川氾濫",
                    "disaster_risk_2_type": "uro:TsunamiRiskAttribute",
                    "disaster_risk_2_description": "大阪府津波浸水想定",
                    "disaster_risk_2_rank": "0.5m以上3m未満",
                    "disaster_risk_2_depth": 2.0,
                    "disaster_risk_2_admin_type": "",
                    "disaster_risk_2_scale": "",
                    "disaster_risk_2_duration": 0.0,
                    "disaster_risk_2_disaster_category": "津波",
                    "disaster_risk_count": 2.0,
                    "disaster_category_河川氾濫_count": 1.0,
                    "disaster_category_津波_count": 1.0,
                    "disaster_category_高潮_count": 0.0,
                    "max_flood_depth": 3.5,
                    "min_flood_depth": 2.0,
                    "avg_flood_depth": 2.75,
                    "specifiedBuildingCoverageRate": 0.8,
                    "surveyYear": "2017",
                    "type": "uro:BuildingDetailAttribute",
                    "buildingRoofEdgeArea": None,
                    "buildingStructureType": None,
                    "buildingStructureOrgType": None,
                    "detailedUsage": None,
                    "groundFloorUsage": None,
                    "secondFloorUsage": None,
                    "thirdFloorUsage": None,
                    "basementFirstUsage": None,
                    "basementSecondUsage": None
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [135.471459927769445, 34.694175673738776]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "class": "堅ろう建物",
                    "measuredHeight": 15.2,
                    "id": "bldg_test_002",
                    "name": "テスト建物2",
                    "disaster_risk_1_type": "uro:HighTideRiskAttribute",
                    "disaster_risk_1_description": "高潮浸水想定区域図",
                    "disaster_risk_1_rank": "5m以上10m未満",
                    "disaster_risk_1_depth": 6.0,
                    "disaster_risk_1_admin_type": "",
                    "disaster_risk_1_scale": "",
                    "disaster_risk_1_duration": 0.0,
                    "disaster_risk_1_disaster_category": "高潮",
                    "disaster_risk_count": 1.0,
                    "disaster_category_河川氾濫_count": 0.0,
                    "disaster_category_津波_count": 0.0,
                    "disaster_category_高潮_count": 1.0,
                    "max_flood_depth": 6.0,
                    "min_flood_depth": 6.0,
                    "avg_flood_depth": 6.0,
                    "specifiedBuildingCoverageRate": 0.6,
                    "surveyYear": "2017",
                    "type": "uro:BuildingDetailAttribute",
                    "buildingRoofEdgeArea": None,
                    "buildingStructureType": None,
                    "buildingStructureOrgType": None,
                    "detailedUsage": None,
                    "groundFloorUsage": None,
                    "secondFloorUsage": None,
                    "thirdFloorUsage": None,
                    "basementFirstUsage": None,
                    "basementSecondUsage": None
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [135.527536743695208, 34.620075482038658]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "class": "普通無壁舎",
                    "measuredHeight": None,
                    "id": "bldg_test_003",
                    "name": None,
                    "disaster_risk_1_type": None,
                    "disaster_risk_1_description": None,
                    "disaster_risk_1_rank": None,
                    "disaster_risk_1_depth": None,
                    "disaster_risk_1_admin_type": None,
                    "disaster_risk_1_scale": None,
                    "disaster_risk_1_duration": None,
                    "disaster_risk_1_disaster_category": None,
                    "disaster_risk_count": None,
                    "disaster_category_河川氾濫_count": None,
                    "disaster_category_津波_count": None,
                    "disaster_category_高潮_count": None,
                    "max_flood_depth": None,
                    "min_flood_depth": None,
                    "avg_flood_depth": None,
                    "specifiedBuildingCoverageRate": 0.6,
                    "surveyYear": "2017",
                    "type": "uro:BuildingDetailAttribute",
                    "buildingRoofEdgeArea": None,
                    "buildingStructureType": None,
                    "buildingStructureOrgType": None,
                    "detailedUsage": None,
                    "groundFloorUsage": None,
                    "secondFloorUsage": None,
                    "thirdFloorUsage": None,
                    "basementFirstUsage": None,
                    "basementSecondUsage": None
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [135.413010581552101, 34.627596425920764]
                }
            }
        ]
    }
    
    # サンプルファイルを保存
    sample_file = Path("Building_sample_test.geojson")
    with open(sample_file, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"サンプルGeoJSONファイルを作成しました: {sample_file}")
    return sample_file

def test_database_connection():
    """データベース接続テスト"""
    try:
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'plateau_buildings'),
            'user': os.getenv('DB_USER', 'h-hibara'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # PostGIS の確認
        cursor.execute("SELECT PostGIS_Version();")
        postgis_version = cursor.fetchone()[0]
        logger.info(f"PostGIS バージョン: {postgis_version}")
        
        # テーブルの確認
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        logger.info(f"既存テーブル: {[table[0] for table in tables]}")
        
        cursor.close()
        conn.close()
        
        logger.info("データベース接続テスト成功")
        return True
        
    except Exception as e:
        logger.error(f"データベース接続テスト失敗: {e}")
        return False

def test_import_process():
    """インポート処理のテスト"""
    try:
        # サンプルファイル作成
        sample_file = create_sample_geojson()
        
        # データベース接続テスト
        if not test_database_connection():
            logger.error("データベース接続に失敗しました")
            return False
        
        # インポート処理のテスト
        from optimized_geojson_importer import OptimizedGeoJSONImporter
        from dotenv import load_dotenv
        
        load_dotenv()
        
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'plateau_buildings'),
            'user': os.getenv('DB_USER', 'h-hibara'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        importer = OptimizedGeoJSONImporter(db_config)
        
        if not importer.connect():
            logger.error("データベース接続に失敗しました")
            return False
        
        # テーブル作成
        if not importer.create_tables():
            logger.error("テーブル作成に失敗しました")
            return False
        
        # サンプルデータのインポート
        if not importer.import_geojson_optimized(str(sample_file), batch_size=10):
            logger.error("サンプルデータのインポートに失敗しました")
            return False
        
        # 結果の確認
        cursor = importer.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM buildings;")
        building_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM disaster_risks;")
        risk_count = cursor.fetchone()[0]
        
        logger.info(f"インポート結果 - 建物数: {building_count}, 災害リスク数: {risk_count}")
        
        # サンプルデータの表示
        cursor.execute("""
            SELECT b.id, b.class, b.measured_height, dr.disaster_category, dr.depth
            FROM buildings b
            LEFT JOIN disaster_risks dr ON b.id = dr.building_id
            ORDER BY b.id, dr.risk_number
        """)
        
        results = cursor.fetchall()
        logger.info("インポートされたデータ:")
        for row in results:
            logger.info(f"  {row}")
        
        importer.disconnect()
        
        # サンプルファイルの削除
        sample_file.unlink()
        
        logger.info("テスト完了")
        return True
        
    except Exception as e:
        logger.error(f"テスト処理エラー: {e}")
        return False

def main():
    """メイン関数"""
    logger.info("インポート処理のテストを開始します")
    
    if test_import_process():
        logger.info("✅ すべてのテストが成功しました")
        logger.info("本番データのインポートを実行できます")
    else:
        logger.error("❌ テストが失敗しました")
        logger.error("設定を確認してから再実行してください")

if __name__ == "__main__":
    main()
