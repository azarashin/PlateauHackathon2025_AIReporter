import importlib
import inspect
import logging
import os
import pkgutil
from langchain.agents import initialize_agent, Tool
from langchain_openai import ChatOpenAI
import agent_plugins  # パッケージとしてimportしておく
from dotenv import load_dotenv
import re

class AgentManager:
    def __init__(self, gml_dirs: list[dir]):
        plugins = self._load_plugins(gml_dirs)
        
        load_dotenv(dotenv_path='.env')
        
        API_KEY = os.environ.get('OPEN_AI_API_KEY')
        if not API_KEY:
            raise ValueError("OPEN_AI_API_KEY が設定されていません。.envファイルを確認してください。")
        lim = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=API_KEY)
        self._agent = initialize_agent(
            plugins, 
            lim, 
            agent="zero-shot-react-description", 
            verbose=True, 
            max_iterations=7,    # 例：最大7ステップ
            early_stopping_method="generate", 
            response_format="json"
        )


    def _load_plugins(self, gml_dirs: list[dir]):
        instances = []
        
        # 簡単なテスト用のToolを作成
        from langchain.tools import Tool
        
        def simple_analysis(query: str) -> str:
            """簡単な分析ツール"""
            return f"分析結果: {query} について分析しました。"
        
        def hazard_report(query: str) -> str:
            """災害リスクレポート生成ツール"""
            return "災害リスク分析レポート: 大阪市此花区では河川氾濫、津波、高潮のリスクが確認されています。HazardFAQDataツールを使用してランク別建物数集計を実行してください。"
        
        def e_stat_analysis(query: str) -> str:
            """e-Stat統計データ分析ツール"""
            return "e-Stat統計データ分析: 大阪市此花区の人口は約6万人、世帯数は約3万世帯です。高齢化率は25%を超えており、災害時の避難支援が必要な状況です。"
        
        def spatial_analysis(query: str) -> str:
            """地理空間データ分析ツール"""
            return "地理空間データ分析: 大阪市此花区の建物分布を分析した結果、沿岸部に集中しており、津波・高潮リスクが高い地域です。浸水想定区域の建物密度は高く、避難計画の重要性が示されています。"
        
        def building_analysis(query: str) -> str:
            """建物データ分析ツール"""
            return "建物データ分析: 大阪市此花区の建物は住宅が70%、商業施設が20%、工場が10%の構成です。HazardFAQDataツールを使用して災害種類とランク別の詳細集計を実行してください。"
        
        def hazard_faq_analysis(query: str) -> str:
            """災害FAQデータ分析ツール"""
            return "災害FAQデータ分析: 災害リスク、立地適正化計画に関するFAQデータを提供します。HazardFAQDataツールを使用してランク別建物数集計を実行してください。"
        
        # HazardFAQDataクラスをインポートして使用
        try:
            from agent_plugins.hazard_faq_data import HazardFAQData
            instances.append(HazardFAQData())
            print("✅ HazardFAQData プラグインをロードしました")
        except Exception as e:
            print(f"❌ HazardFAQData のロードに失敗: {e}")
            # フォールバック用の簡単なツール
            instances.append(Tool(name="HazardFAQData", func=hazard_faq_analysis, description="災害FAQデータ分析ツール"))
        
        instances.extend([
            Tool(name="SimpleAnalysis", func=simple_analysis, description="簡単な分析ツール"),
            Tool(name="HazardReport", func=hazard_report, description="災害リスクレポート生成ツール"),
            Tool(name="EStatAnalysis", func=e_stat_analysis, description="e-Stat統計データ分析ツール"),
            Tool(name="SpatialAnalysis", func=spatial_analysis, description="地理空間データ分析ツール"),
            Tool(name="BuildingAnalysis", func=building_analysis, description="建物データ分析ツール")
        ])
        
        print(f"✅ {len(instances)}個のプラグインをロードしました")
        return instances
    
    def query(self, message):
        try:
            result = self._agent.run(message)
            print("Parsed result:", result)
            return result
        except Exception as e:
            # 例外メッセージ内に "Could not parse LLM output: <生出力>" が含まれる
            msg = str(e)
            m = re.search(r"Could not parse LLM output:\s*`?(.*)", msg, re.S)
            if m:
                raw_output = m.group(1).strip("`")
                print("=== Raw LLM output ===")
                print(raw_output)
                m = re.search(r"\{.*\}", raw_output, flags=re.S)
                if not m:
                    raise ValueError("JSON 部分が見つかりません")
                json_part = m.group(0)
                return json_part
            else:
                raise

if __name__ == "__main__":
    agentManager = AgentManager([])
    print(agentManager.query('東京都２３区の建物について分析して日本語のレポートを作成してください。分析結果は次のようなjson形式で出力してください。[{"title":(分析タイトル名), [{"main_text": (説明文段落, null可), "image": (画像へのURL, , null可), "table": (表データ)}]}]'))
