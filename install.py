from pathlib import Path
import sys

from AIAgentForCityGML.city_gml2meta import extract_gml_metadata_and_features
from AIAgentForCityGML.gml_stats_all import extract_gml_stats


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

        bins = 10
        out_stat_json = gml.with_suffix(".stat.json")
        extract_gml_stats(gml, out_stat_json, bins=bins)

if __name__ == "__main__":
    main()