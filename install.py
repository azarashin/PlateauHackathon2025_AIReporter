from pathlib import Path
import sys
import argparse

from AIAgentForCityGML.city_gml2meta import extract_gml_metadata_and_features
from AIAgentForCityGML.gml_stats_all import extract_gml_stats


def main():
    parser = argparse.ArgumentParser(description="CityGML データを元に前処理を実施する")

    # overwrite フラグ（True/False）
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="このフラグを指定すると、前処理によって生成されるファイルを上書きします"
    )

    # ディレクトリパス（最低1つ以上）
    parser.add_argument(
        "directories",
        nargs="+",  # 1つ以上の引数を必須にする
        type=Path,
        help="CityGMLのディレクトリパス（最低1つ必要）"
    )

    args = parser.parse_args()

    print(f"overwrite = {args.overwrite}")
    print("指定されたディレクトリ:")
    for dir_path in args.directories:
        print(f" - {dir_path}（存在: {dir_path.exists()}, ディレクトリ: {dir_path.is_dir()})")

    gml_files = {}
    for base_dir in args.directories:
        if not base_dir.is_dir():
            print(f"ディレクトリが見つかりません: {base_dir}")
            return

        new_gml_files = list(base_dir.rglob("*.gml"))
        if not new_gml_files:
            print("*.gml ファイルが見つかりません。")
            return
        gml_files[base_dir] = new_gml_files

    for base_dir in args.directories:
        next_gml_files = gml_files[base_dir]
        for gml in next_gml_files:
            out_json = gml.with_suffix(".json")
            extract_gml_metadata_and_features(gml, out_json)

            bins = 10
            out_stat_json = gml.with_suffix(".stat.json")
            extract_gml_stats(gml, out_stat_json, bins=bins)

if __name__ == "__main__":
    main()