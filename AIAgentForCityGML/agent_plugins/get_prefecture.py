import os
import sys
from pathlib import Path

from AIAgentForCityGML.gml_stats_manager import GmlStatManager
from langchain.agents import Tool

class GetPrefecture(Tool):
    def __init__(self, gml_dirs: list[dir]):
        super().__init__( 
            name="MakeCityList", 
            func=self._get_prefecture, 
            description="分析対象となりうる県の名前の一覧を取得します。このエージェントには入力として必要な情報はなく、[名前1, 名前2, ...]のように県の名前の文字列がリストになったものを応答として返します。")
        self._gml_dirs = gml_dirs
        ret = []
        target_attrib = 'prefecture'
        for dir in self._gml_dirs:
            gml_stat_manager = GmlStatManager(dir)
            for stat in gml_stat_manager.stat_list:
                for layer_idx in range(stat.get_layer_count()):
                    val = stat.get_string_attribute_mean(layer_idx, target_attrib)
                    ret.extend(val.keys())
        self._prefecture_list = list(set(ret))

    def _get_prefecture(self, expression: str) -> str:
        return self._prefecture_list


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("使い方: python extract_gml_features_all.py <ディレクトリパス>")
        sys.exit(1)

    print(sys.argv[1])
    print(os.path.isdir(sys.argv[1]))
    base_dir = Path(sys.argv[1]).resolve()
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)
    agent = GetPrefecture([base_dir])
    print(agent._get_prefecture(""))
    
       