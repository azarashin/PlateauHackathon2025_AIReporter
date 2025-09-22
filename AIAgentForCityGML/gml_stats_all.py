#!/usr/bin/env python3
import json
import sys
import os
from pathlib import Path
from statistics import mean
from collections import Counter
from osgeo import ogr

# 強制的に文字列として扱うフィールド
FORCE_STRING_FIELDS = {"usage", "prefecture", "city", "rank", "surveyYear"}
# 完全に無視するフィールド
IGNORE_FIELDS = {"buildingID", "gml_id"}

def compute_histogram(values, bins=10):
    """数値リストから簡易ヒストグラムを作成"""
    if not values:
        return {"bin_edges": [], "counts": []}

    vmin, vmax = min(values), max(values)
    if vmin == vmax:
        return {"bin_edges": [vmin, vmax], "counts": [len(values)]}

    step = (vmax - vmin) / bins
    edges = [vmin + step * i for i in range(bins + 1)]
    counts = [0] * bins
    for v in values:
        idx = min(int((v - vmin) / step), bins - 1)
        counts[idx] += 1
    return {"bin_edges": edges, "counts": counts}

def extract_gml_stats(input_gml: Path, output_json: Path, bins: int = 10, overwrite: bool = False):
    if not overwrite and os.path.exists(output_json):
        print(f"[Skip] {input_gml} → {output_json}")
        return
    """
    1つのGMLファイル内の各フィールドについて
      - 数値フィールド: count/min/max/mean/histogram
      - 文字列フィールド: 出現頻度
    を計算して JSON へ保存
    """
    ds = ogr.Open(str(input_gml))
    if ds is None:
        print(f"[WARN] ファイルを開けません: {input_gml}")
        return

    result = {
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

        numeric_fields = []
        string_fields  = []

        # フィールド分類
        for j in range(layer_defn.GetFieldCount()):
            fdef = layer_defn.GetFieldDefn(j)
            fname = fdef.GetName()
            if fname in IGNORE_FIELDS:
                continue  # buildingID, gml_id は無視
            ftype = fdef.GetTypeName()

            # usage, prefecture, city, rank, surveyYear, 末尾がType → 強制的に文字列扱い
            if (fname.lower() in {f.lower() for f in FORCE_STRING_FIELDS}) or fname.lower().endswith("type"):
                string_fields.append(fname)
            elif ftype in ("Integer", "Real", "Integer64"):
                numeric_fields.append(fname)
            else:
                string_fields.append(fname)

        # データ収集用
        numeric_data = {fname: [] for fname in numeric_fields}
        string_data  = {fname: [] for fname in string_fields}

        layer.ResetReading()
        for feature in layer:
            for fname in numeric_fields:
                val = feature.GetField(fname)
                if val is not None:
                    numeric_data[fname].append(val)
            for fname in string_fields:
                val = feature.GetField(fname)
                if val is not None:
                    string_data[fname].append(str(val))

        # 数値フィールド統計
        numeric_stats = {}
        for fname, values in numeric_data.items():
            if values:
                numeric_stats[fname] = {
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "mean": mean(values),
                    "histogram": compute_histogram(values, bins)
                }
            else:
                numeric_stats[fname] = {
                    "count": 0,
                    "min": None,
                    "max": None,
                    "mean": None,
                    "histogram": {"bin_edges": [], "counts": []}
                }

        # 文字列フィールド出現頻度
        string_stats = {}
        for fname, values in string_data.items():
            counter = Counter(values)
            string_stats[fname] = dict(counter.most_common())  # 頻度降順

        layer_info = {
            "name": layer.GetName(),
            "feature_count": layer.GetFeatureCount(),
            "spatial_ref_wkt": srs_wkt,
            "numeric_field_stats": numeric_stats,
            "string_field_frequencies": string_stats
        }
        result["layers"].append(layer_info)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"[OK] {input_gml} → {output_json}")

def main():
    if len(sys.argv) < 2:
        print("使い方: python gml_stats_all.py <ディレクトリパス> [bins]")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)

    bins = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    gml_files = list(base_dir.rglob("*.gml"))
    if not gml_files:
        print("*.gml ファイルが見つかりません。")
        return

    for gml in gml_files:
        out_json = gml.with_suffix(".stat.json")
        extract_gml_stats(gml, out_json, bins=bins)

if __name__ == "__main__":
    main()
