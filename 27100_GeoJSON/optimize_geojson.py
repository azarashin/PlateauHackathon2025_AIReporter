#!/usr/bin/env python3
"""
GeoJSON最適化スクリプト
- Polygonを重心点に変換
- 属性を平坦化
- ファイルサイズを削減
"""

import json
import sys
import os
from typing import Dict, Any
import geopandas as gpd
import pandas as pd

def flatten_disaster_risk_attribute(risk_attr_str: str) -> Dict[str, Any]:
    """災害リスク属性を平坦化"""
    if not risk_attr_str or risk_attr_str == 'null':
        return {}
    
    try:
        risk_data = json.loads(risk_attr_str)
        if not isinstance(risk_data, list):
            return {}
        
        flattened = {}
        
        # 各災害リスクを個別のフィールドに展開（最大3つまで）
        for i, risk in enumerate(risk_data[:3]):  # 最大3つの災害リスクまで
            prefix = f"disaster_risk_{i+1}_"
            
            # 基本情報
            flattened[f"{prefix}type"] = risk.get("type", "")
            flattened[f"{prefix}description"] = risk.get("description", "")
            flattened[f"{prefix}rank"] = risk.get("rank", "")
            flattened[f"{prefix}depth"] = risk.get("depth", 0)
            flattened[f"{prefix}admin_type"] = risk.get("adminType", "")
            flattened[f"{prefix}scale"] = risk.get("scale", "")
            flattened[f"{prefix}duration"] = risk.get("duration", 0)
            
            # 災害タイプ別の分類
            risk_type = risk.get("type", "")
            if "RiverFlooding" in risk_type:
                flattened[f"{prefix}disaster_category"] = "河川氾濫"
            elif "Tsunami" in risk_type:
                flattened[f"{prefix}disaster_category"] = "津波"
            elif "HighTide" in risk_type:
                flattened[f"{prefix}disaster_category"] = "高潮"
            else:
                flattened[f"{prefix}disaster_category"] = "その他"
        
        # 災害リスクの総数
        flattened["disaster_risk_count"] = len(risk_data)
        
        # 災害カテゴリ別の集計
        categories = {}
        for risk in risk_data:
            risk_type = risk.get("type", "")
            if "RiverFlooding" in risk_type:
                categories["河川氾濫"] = categories.get("河川氾濫", 0) + 1
            elif "Tsunami" in risk_type:
                categories["津波"] = categories.get("津波", 0) + 1
            elif "HighTide" in risk_type:
                categories["高潮"] = categories.get("高潮", 0) + 1
            else:
                categories["その他"] = categories.get("その他", 0) + 1
        
        for category, count in categories.items():
            flattened[f"disaster_category_{category}_count"] = count
        
        # 最大浸水深
        depths = [risk.get("depth", 0) for risk in risk_data if risk.get("depth")]
        if depths:
            flattened["max_flood_depth"] = max(depths)
            flattened["min_flood_depth"] = min(depths)
            flattened["avg_flood_depth"] = sum(depths) / len(depths)
        
        return flattened
        
    except (json.JSONDecodeError, TypeError) as e:
        print(f"JSON解析エラー: {e}")
        return {}

def flatten_building_detail_attribute(detail_attr_str: str) -> Dict[str, Any]:
    """建物詳細属性を平坦化"""
    if not detail_attr_str or detail_attr_str == 'null':
        return {}
    
    try:
        detail_data = json.loads(detail_attr_str)
        if not isinstance(detail_data, list) or len(detail_data) == 0:
            return {}
        
        flattened = {}
        
        # 建物詳細属性の全フィールドを展開
        detail = detail_data[0]  # 最初のオブジェクト
        
        # 全てのキーと値をそのまま展開
        for key, value in detail.items():
            flattened[key] = value
        
        return flattened
        
    except (json.JSONDecodeError, TypeError) as e:
        print(f"建物詳細属性JSON解析エラー: {e}")
        return {}

def optimize_geojson(input_file: str, output_file: str):
    """GeoJSONを最適化"""
    print(f"処理開始: {input_file}")
    
    try:
        # GeoPandasでGeoJSONを読み込み
        gdf = gpd.read_file(input_file)
        print(f"総レコード数: {len(gdf)}")
        
        # 指定されたフィールドのみを保持（usageフィールドは除外）
        keep_fields = ['class', 'measuredHeight', 'id', 'name', 'buildingDisasterRiskAttribute', 'buildingDetailAttribute']
        available_fields = [field for field in keep_fields if field in gdf.columns]
        
        # usageフィールドが存在する場合は削除
        if 'usage' in gdf.columns:
            print("usageフィールドを削除します")
            gdf = gdf.drop(columns=['usage'])
        
        print(f"保持するフィールド: {available_fields}")
        
        # 必要なフィールドのみを選択
        gdf_filtered = gdf[available_fields + ['geometry']].copy()
        
        # PolygonをCentroidに変換
        print("PolygonをCentroidに変換中...")
        gdf_filtered['geometry'] = gdf_filtered['geometry'].centroid
        
        # 属性を平坦化
        print("属性を平坦化中...")
        disaster_risk_count = 0
        building_detail_count = 0
        building_detail_fields = set()
        
        for idx, row in gdf_filtered.iterrows():
            # 災害リスク属性の処理
            risk_attr = row.get('buildingDisasterRiskAttribute')
            if risk_attr and pd.notna(risk_attr):
                disaster_risk_count += 1
                flattened_risk = flatten_disaster_risk_attribute(risk_attr)
                for key, value in flattened_risk.items():
                    gdf_filtered.at[idx, key] = value
            
            # 建物詳細属性の処理
            detail_attr = row.get('buildingDetailAttribute')
            if detail_attr and pd.notna(detail_attr):
                building_detail_count += 1
                flattened_detail = flatten_building_detail_attribute(detail_attr)
                for key, value in flattened_detail.items():
                    gdf_filtered.at[idx, key] = value
                    building_detail_fields.add(key)
            
            if (idx + 1) % 50000 == 0:
                print(f"処理済み: {idx + 1}件")
        
        # 元の構造化されたフィールドを削除
        columns_to_drop = ['buildingDisasterRiskAttribute', 'buildingDetailAttribute']
        gdf_filtered = gdf_filtered.drop(columns=[col for col in columns_to_drop if col in gdf_filtered.columns])
        
        # 結果を保存
        print(f"最適化されたGeoJSONを保存中: {output_file}")
        gdf_filtered.to_file(output_file, driver='GeoJSON')
        
        # ファイルサイズの比較
        original_size = os.path.getsize(input_file)
        optimized_size = os.path.getsize(output_file)
        reduction_ratio = (1 - optimized_size / original_size) * 100
        
        print(f"\n=== 最適化結果 ===")
        print(f"元のファイルサイズ: {original_size:,} bytes ({original_size/1024/1024:.1f} MB)")
        print(f"最適化後ファイルサイズ: {optimized_size:,} bytes ({optimized_size/1024/1024:.1f} MB)")
        print(f"サイズ削減率: {reduction_ratio:.1f}%")
        print(f"災害リスク属性を持つ建物数: {disaster_risk_count}")
        print(f"建物詳細属性を持つ建物数: {building_detail_count}")
        print(f"展開された建物詳細属性フィールド: {sorted(building_detail_fields)}")
        print(f"最終フィールド数: {len(gdf_filtered.columns)}")
        
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("使用方法: python optimize_geojson.py <入力ファイル> <出力ファイル>")
        print("例: python optimize_geojson.py Building.geojson Building_optimized.geojson")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    optimize_geojson(input_file, output_file)

if __name__ == "__main__":
    main()
