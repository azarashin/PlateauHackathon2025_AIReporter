#!/usr/bin/env python3
"""
buildingDisasterRiskAttributeを階層を開いたGeoJSONに変換するスクリプト
可読性を高めるために、構造化されたJSONを平坦化する
"""

import json
import sys
from typing import Dict, List, Any

def flatten_disaster_risk_attribute(risk_attr_str: str) -> Dict[str, Any]:
    """
    災害リスク属性を平坦化する
    
    Args:
        risk_attr_str: 災害リスク属性のJSON文字列
        
    Returns:
        平坦化された災害リスク属性の辞書
    """
    if not risk_attr_str or risk_attr_str == 'null':
        return {}
    
    try:
        risk_data = json.loads(risk_attr_str)
        if not isinstance(risk_data, list):
            return {}
        
        flattened = {}
        
        # 各災害リスクを個別のフィールドに展開
        for i, risk in enumerate(risk_data):
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

def process_geojson(input_file: str, output_file: str):
    """
    GeoJSONファイルを処理して災害リスク属性を平坦化する
    
    Args:
        input_file: 入力GeoJSONファイル
        output_file: 出力GeoJSONファイル
    """
    print(f"処理開始: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"総レコード数: {len(geojson_data['features'])}")
        
        processed_count = 0
        disaster_risk_count = 0
        
        for feature in geojson_data['features']:
            properties = feature.get('properties', {})
            risk_attr = properties.get('buildingDisasterRiskAttribute')
            
            if risk_attr:
                disaster_risk_count += 1
                flattened_risk = flatten_disaster_risk_attribute(risk_attr)
                
                # 平坦化された属性を追加
                for key, value in flattened_risk.items():
                    properties[key] = value
                
                # 元の構造化された属性は残す（コメントアウトで無効化）
                # properties['buildingDisasterRiskAttribute_original'] = risk_attr
                # del properties['buildingDisasterRiskAttribute']
            
            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"処理済み: {processed_count}件")
        
        # 結果を保存
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
        
        print(f"処理完了: {output_file}")
        print(f"災害リスク属性を持つ建物数: {disaster_risk_count}")
        
    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("使用方法: python flatten_disaster_risk.py <入力ファイル> <出力ファイル>")
        print("例: python flatten_disaster_risk.py Building.geojson Building_flattened.geojson")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_geojson(input_file, output_file)

if __name__ == "__main__":
    main()
