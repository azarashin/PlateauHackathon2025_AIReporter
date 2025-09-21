import json
from pathlib import Path
from langchain.tools import Tool


class SimulateHazardRisk(Tool):
    # --- 拡張性のための設計コメント ---
    # 今後、matplotlibやfolium等を用いた地図・グラフ画像生成機能を追加可能
    # 例: 建物分布の地図可視化、災害リスク分布のヒートマップ、集計グラフなど

    def generate_map_image(self, output_path=None, **kwargs):
        """
        （拡張用）建物やリスク分布の地図画像を生成するメソッド枠
        output_path: 画像保存先パス
        kwargs: 可視化条件（例：災害カテゴリ、浸水深の閾値など）
        """
        # ここにfoliumやmatplotlib等の地図・グラフ生成処理を実装予定
        # 例: foliumで地図を生成し、建物位置やリスクを色分け表示
        # 例: matplotlibで棒グラフや円グラフを生成
        pass
    def generate_resident_report(self):
        """
        住民説明用のテキスト資料を自動生成
        """
        total = len(self.features)
        flood_3m = self.count_buildings_with_flood_resistance(3.0)
        flood_1m = len(self.query_buildings(min_flood_depth=1.0))
        summary = self.summary_by_disaster_category()
        lines = []
        lines.append(f"本地区（大阪市内）には、合計{total}棟の建物データが登録されています。")
        lines.append(f"このうち、3m以上の高さがあり、河川氾濫リスクが想定される建物は{flood_3m}棟です。")
        lines.append(f"また、1m以上の浸水リスクがある建物は{flood_1m}棟存在します。")
        lines.append("災害種別ごとの建物件数は以下の通りです：")
        for cat, cnt in summary.items():
            lines.append(f"  - {cat}: {cnt}棟")
        lines.append("これらの情報は、立地適正化計画や防災対策の検討、住民の皆様への説明資料としてご活用いただけます。")
        return "\n".join(lines)
    def __init__(self, geojson_path):
        self.geojson_path = Path(geojson_path)
        self.features = []
        self._load_data()
        
        # Toolクラスの初期化
        super().__init__(
            name="SimulateHazardRisk",
            func=self._run,
            description="災害リスク分析と住民説明資料生成ツール"
        )
    
    def _run(self, query: str) -> str:
        """Toolの実行メソッド"""
        try:
            if "住民説明" in query or "レポート" in query:
                return self.generate_resident_report()
            elif "建物件数" in query:
                return f"3m以上の建物件数: {self.count_buildings_with_flood_resistance(3.0)}"
            elif "災害カテゴリ" in query:
                summary = self.summary_by_disaster_category()
                return f"災害カテゴリ別集計: {summary}"
            else:
                return self.generate_resident_report()
        except Exception as e:
            return f"エラーが発生しました: {e}"

    def _load_data(self):
        try:
            with open(self.geojson_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.features = data.get('features', [])
        except Exception as e:
            print(f"Error loading geojson: {e}")
            self.features = []

    def query_buildings(self, min_height=None, max_height=None, usage=None, disaster_category=None, min_flood_depth=None, max_flood_depth=None):
        """
        条件に合致する建物リストを返す
        - min_height, max_height: 建物高さ（m）
        - usage: 用途（detailedUsage, groundFloorUsage等）
        - disaster_category: 災害カテゴリ（例：河川氾濫）
        - min_flood_depth, max_flood_depth: 浸水深（m）
        """
        results = []
        for feat in self.features:
            prop = feat.get('properties', {})
            # 高さ条件
            height = prop.get('measuredHeight')
            if height is not None:
                try:
                    height = float(height)
                except Exception:
                    continue
            if min_height is not None and (height is None or height < min_height):
                continue
            if max_height is not None and (height is None or height > max_height):
                continue
            # 用途条件
            if usage:
                usages = [prop.get('detailedUsage'), prop.get('groundFloorUsage'), prop.get('secondFloorUsage'), prop.get('thirdFloorUsage')]
                if usage not in usages:
                    continue
            # 災害カテゴリ条件
            if disaster_category:
                found = False
                for i in range(1, 4):
                    cat = prop.get(f'disaster_risk_{i}_disaster_category')
                    if cat and disaster_category in cat:
                        found = True
                        break
                if not found:
                    continue
            # 浸水深条件
            flood_depth = prop.get('max_flood_depth')
            if flood_depth is not None:
                try:
                    flood_depth = float(flood_depth)
                except Exception:
                    continue
            if min_flood_depth is not None and (flood_depth is None or flood_depth < min_flood_depth):
                continue
            if max_flood_depth is not None and (flood_depth is None or flood_depth > max_flood_depth):
                continue
            results.append(feat)
        return results

    def count_buildings_with_flood_resistance(self, min_height=3.0):
        """
        指定した高さ以上の建物で、洪水リスク情報があるものの件数を返す
        """
        return len(self.query_buildings(min_height=min_height, disaster_category="河川氾濫"))

    def summary_by_disaster_category(self):
        """
        災害カテゴリごとの建物件数を集計
        """
        summary = {}
        for feat in self.features:
            prop = feat.get('properties', {})
            for i in range(1, 4):
                cat = prop.get(f'disaster_risk_{i}_disaster_category')
                if cat:
                    summary[cat] = summary.get(cat, 0) + 1
        return summary

    def example_prompt_queries(self):
        """
        代表的なプロンプト例の実行サンプル
        """
        results = {}
        # 例1: 3m以上の建物で河川氾濫リスクあり
        results['3m以上の建物件数'] = self.count_buildings_with_flood_resistance(3.0)
        # 例2: 1m以上浸水リスクのある建物
        results['1m以上浸水リスク建物'] = len(self.query_buildings(min_flood_depth=1.0))
        # 例3: 用途が住宅の建物
        results['住宅用途の建物'] = len(self.query_buildings(usage="住宅"))
        return results

# --- サンプル利用例 ---
if __name__ == "__main__":
    geojson_path = "../../CityGMLData/Building_optimized.geojson"
    agent = SimulateHazardRisk(geojson_path)
    print("3m以上の建物件数:", agent.count_buildings_with_flood_resistance(3.0))
    print("災害カテゴリ別集計:", agent.summary_by_disaster_category())
    print("代表的なプロンプト例:", agent.example_prompt_queries())
    print("\n--- 住民説明用資料サンプル ---")
    print(agent.generate_resident_report())
