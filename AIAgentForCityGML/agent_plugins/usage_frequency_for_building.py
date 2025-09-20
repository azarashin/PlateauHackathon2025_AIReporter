import os
import sys
from pathlib import Path

from AIAgentForCityGML.agent_plugins.get_attrib_frequency import GetStringAttributeFrequency
from AIAgentForCityGML.gml_stats_manager import GmlStatManager
from langchain.agents import Tool

class UsageFrequencyForBuilding(GetStringAttributeFrequency):
    def __init__(self, gml_dirs: list[dir]):
        super().__init__(gml_dirs, 'Building', 'usage', '用途')

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
    agent = UsageFrequencyForBuilding([base_dir])
    print(agent.get_attribute_frequency(""))
    
       