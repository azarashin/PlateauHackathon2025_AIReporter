#!/usr/bin/env python3
import json
import sys
import os
from pathlib import Path
from osgeo import ogr

'''
.gml データからメタデータを抽出して出力する
'''


def extract_gml_metadata_and_features(input_gml: Path, output_json: Path, overwrite: bool = False):
    if not overwrite and os.path.exists(output_json):
        print(f"[Skip] {input_gml} → {output_json}")
        return
    """
    1つの GML ファイルから
      - レイヤメタデータ
      - 全フィーチャの属性値とジオメトリのバウンディングボックス
    を抽出して JSON に保存
    """
    ds = ogr.Open(str(input_gml))
    if ds is None:
        print(f"[WARN] ファイルを開けません: {input_gml}")
        return

    file_info = {
        "source": str(input_gml.resolve()),
        "driver": ds.GetDriver().GetName(),
        "layer_count": ds.GetLayerCount(),
        "layers": []
    }

    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        srs = layer.GetSpatialRef()
        srs_wkt = srs.ExportToWkt() if srs else None

        layer_defn = layer.GetLayerDefn()
        fields = []
        for j in range(layer_defn.GetFieldCount()):
            fdef = layer_defn.GetFieldDefn(j)
            fields.append({
                "name": fdef.GetName(),
                "type": fdef.GetTypeName(),
                "width": fdef.GetWidth(),
                "precision": fdef.GetPrecision()
            })

        # ---- フィーチャ情報を収集 ----
        features_info = []
        layer.ResetReading()
        for feature in layer:
            # 属性を辞書に
            attrs = {}
            for j in range(layer_defn.GetFieldCount()):
                fname = layer_defn.GetFieldDefn(j).GetName()
                attrs[fname] = feature.GetField(fname)

            # ジオメトリのバウンディングボックス（必要に応じてWKT等も可）
            geom = feature.GetGeometryRef()
            if geom:
                bbox = geom.GetEnvelope()  # (minX, maxX, minY, maxY)
            else:
                bbox = None

            features_info.append({
                "fid": feature.GetFID(),
                "attributes": attrs,
                "geometry_bbox": bbox
            })

        layer_info = {
            "name": layer.GetName(),
            "feature_count": layer.GetFeatureCount(),
            "geometry_type": ogr.GeometryTypeToName(layer.GetGeomType()),
            "spatial_ref_wkt": srs_wkt,
            "fields": fields,
            "extent": layer.GetExtent(),
            "features": features_info
        }
        file_info["layers"].append(layer_info)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(file_info, f, indent=2, ensure_ascii=False)

    print(f"[OK] {input_gml} → {output_json}")

def main():
    if len(sys.argv) != 2:
        print("使い方: python extract_gml_features_all.py <ディレクトリパス>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)

    gml_files = list(base_dir.rglob("*.gml"))
    if not gml_files:
        print("*.gml ファイルが見つかりません。")
        return

    for gml in gml_files:
        out_json = gml.with_suffix(".json")
        extract_gml_metadata_and_features(gml, out_json)

if __name__ == "__main__":
    main()