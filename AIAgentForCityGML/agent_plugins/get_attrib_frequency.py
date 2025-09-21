import os
import sys
from pathlib import Path

from AIAgentForCityGML.gml_stats_manager import GmlStatManager
from langchain.agents import Tool

class GetStringAttributeFrequency(Tool):
    def __init__(self, gml_dirs: list[dir], target_layer: str, attrib_id: str, attrib_mean: str):
        super().__init__( 
            name=f"GetStringAttributeFrequency_{attrib_id}", 
            func=self.get_attribute_frequency, 
            description=f"レイヤー{target_layer}に対して非可算属性（{attrib_mean}）の各属性値の発生頻度を求めます。このエージェントには入力として必要な情報はなく、[属性値1:頻度1, 属性値2:頻度2, ...]のように属性値の名前とその属性値の発生頻度がハッシュになったものを応答として返します。")
        self._attrib_id = attrib_id
        self._gml_dirs = gml_dirs
        self._gml_stat_manager = {}
        for dir in self._gml_dirs:
            self._gml_stat_manager[dir] = GmlStatManager(dir)
        ret = {}
        for dir in self._gml_stat_manager:
            for stat in self._gml_stat_manager[dir].stat_list:
                for layer_idx in range(stat.get_layer_count()):
                    if stat.get_layer_name(layer_idx) != target_layer:
                        continue
                    attrib_freq = stat.get_string_attribute_mean(layer_idx, self._attrib_id)
                    for attrib in attrib_freq:
                        if not attrib in ret:
                            ret[attrib] = 0
                        ret[attrib] += attrib_freq[attrib]
        self._frequency = ret

    def get_attribute_frequency(self, expression: str) -> str:
        print(self._frequency)
        return self._frequency
