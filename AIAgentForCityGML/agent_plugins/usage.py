from langchain.agents import Tool

class Usage(Tool):
    def __init__(self, gml_dirs: list[dir]):
        super().__init__( 
            name="Usage", 
            func=self._usage, 
            description="指定された緯度・経度の特定半径内にある建物の数を調べる。問い合わせはJSON 形式で受付け、緯度:lat, 経度:lng, 半径:radius が含まれる。")

    def _usage(self, expression: str) -> str:
        return [
            {"都市名": "東東京市", "住宅": 1000, "商業施設": 1200}, 
            {"都市名": "大田区", "住宅": 1200, "商業施設": 1300}
        ]