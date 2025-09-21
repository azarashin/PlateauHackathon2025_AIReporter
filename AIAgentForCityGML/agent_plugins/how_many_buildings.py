from langchain.agents import Tool

class HowManyBuildings(Tool):
    def __init__(self, gml_dirs: list[dir]):
        super().__init__( 
            name="HowManyBuildings", 
            func=self._how_many_buildings, 
            description="指定された緯度・経度の特定半径内にある建物の数を調べる。問い合わせはJSON 形式で受付け、緯度:lat, 経度:lng, 半径:radius が含まれる。")

    def _how_many_buildings(self, expression: str) -> str:
        return 100
