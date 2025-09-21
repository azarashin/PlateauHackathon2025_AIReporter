# 立地適正化計画・都市防災FAQ（サンプル）
# 住民説明会や行政窓口でよくある質問例

from langchain.tools import Tool
import duckdb
import json
import os

faqs = [
    {
        "question": "この地域はどのような災害リスクがありますか？",
        "answer": "本地区は河川氾濫、津波、高潮など複数の災害リスクが想定されています。各建物ごとのリスク情報は資料をご参照ください。"
    },
    {
        "question": "自宅が浸水リスク区域に該当していますが、どの程度の対策が必要ですか？",
        "answer": "想定浸水深や建物の高さ・構造に応じて、避難計画や耐水対策の検討を推奨します。"
    },
    {
        "question": "立地適正化計画とは何ですか？",
        "answer": "災害リスクや都市機能を考慮し、居住や都市機能の誘導区域を定める計画です。安全・安心なまちづくりのための指針となります。"
    },
    {
        "question": "今後、建物の用途や構造を変更したい場合、どのような手続きが必要ですか？",
        "answer": "用途変更や構造改修には、建築基準法や都市計画法等に基づく手続きが必要です。詳細は行政窓口にご相談ください。"
    },
    {
        "question": "この資料は他の地域でも使えますか？",
        "answer": "同様のデータが整備されていれば、他都市・他地域でも横展開が可能です。"
    },
    {
        "question": "建物の築年数や改修履歴もリスク評価に使えますか？",
        "answer": "該当データがあれば、耐震性や耐水性の評価に活用できます。今後の拡張も検討しています。"
    }
]

class HazardFAQData(Tool):
    def __init__(self):
        super().__init__(
            name="HazardFAQData",
            func=self._run,
            description="災害種類とランク別建物数集計・対策立案ツール（DuckDB連携）。河川氾濫、津波、高潮のリスクランク別建物数と割合を詳細に分析し、具体的な対策案を生成します。ハザードリスク、ランク別建物数、集計、対策の分析に最適です。"
        )
        # クラス変数として設定
        HazardFAQData.db_path = "geo.duckdb"
        self._init_database()
    
    def _init_database(self):
        """DuckDBデータベースの初期化"""
        try:
            if os.path.exists(HazardFAQData.db_path):
                print(f"✅ DuckDBデータベースが見つかりました: {HazardFAQData.db_path}")
            else:
                print(f"❌ DuckDBデータベースが見つかりません: {HazardFAQData.db_path}")
        except Exception as e:
            print(f"❌ データベース初期化エラー: {e}")
    
    def _run(self, query: str) -> str:
        """Toolの実行メソッド"""
        try:
            # 災害種類とランク別建物数集計の判定を強化
            if any(keyword in query for keyword in ["災害種類", "災害ランク", "建物数", "集計", "此花区", "大阪市"]):
                if "対策" in query or "立案" in query or "LLM" in query:
                    return self.generate_countermeasures_with_llm(query)
                elif "詳細" in query or "分析" in query:
                    return self.get_disaster_rank_analysis(query)
                else:
                    return self.get_disaster_building_count(query)
            elif "FAQ" in query or "よくある質問" in query:
                return self.get_faq_summary()
            elif "災害リスク" in query:
                return self.get_disaster_risk_faq()
            elif "立地適正化" in query:
                return self.get_urban_planning_faq()
            else:
                # デフォルトで災害建物数集計を実行
                return self.get_disaster_building_count(query)
        except Exception as e:
            return f"エラーが発生しました: {e}"
    
    def get_faq_summary(self):
        """FAQの概要を返す"""
        return f"災害FAQデータ: {len(faqs)}件の質問と回答を提供しています。"
    
    def get_disaster_risk_faq(self):
        """災害リスク関連のFAQを返す"""
        disaster_faqs = [faq for faq in faqs if "災害" in faq["question"] or "リスク" in faq["question"]]
        result = "災害リスク関連FAQ:\n"
        for faq in disaster_faqs:
            result += f"Q: {faq['question']}\nA: {faq['answer']}\n\n"
        return result
    
    def get_urban_planning_faq(self):
        """立地適正化計画関連のFAQを返す"""
        planning_faqs = [faq for faq in faqs if "立地適正化" in faq["question"] or "用途" in faq["question"]]
        result = "立地適正化計画関連FAQ:\n"
        for faq in planning_faqs:
            result += f"Q: {faq['question']}\nA: {faq['answer']}\n\n"
        return result
    
    def get_disaster_building_count(self, query: str) -> str:
        """指定された地域の災害種類とランク別に建物数を集計（値と割合）"""
        try:
            # DuckDBに接続
            conn = duckdb.connect(HazardFAQData.db_path)
            
            # 地域名を抽出（クエリから）
            region = "大阪市此花区"  # デフォルト値
            if "此花区" in query:
                region = "大阪市此花区"
            elif "大阪市" in query:
                region = "大阪市"

            # 1. 全体の建物数を取得
            total_sql = """
            SELECT COUNT(*) as total_buildings
            FROM building_optimized
            """
            total_result = conn.execute(total_sql).fetchone()
            total_buildings = total_result[0] if total_result else 0

            if total_buildings == 0:
                conn.close()
                return f"❌ {region}の建物データが見つかりませんでした。"

            # 2. 災害種類とランク別の建物数集計クエリ
            sql_query = """
            SELECT
                disaster_risk_1_disaster_category as disaster_category,
                disaster_risk_1_rank as disaster_rank,
                COUNT(*) as building_count
            FROM building_optimized
            WHERE disaster_risk_1_disaster_category IS NOT NULL
            GROUP BY disaster_risk_1_disaster_category, disaster_risk_1_rank
            UNION ALL
            SELECT
                disaster_risk_2_disaster_category as disaster_category,
                disaster_risk_2_rank as disaster_rank,
                COUNT(*) as building_count
            FROM building_optimized
            WHERE disaster_risk_2_disaster_category IS NOT NULL
            GROUP BY disaster_risk_2_disaster_category, disaster_risk_2_rank
            UNION ALL
            SELECT
                disaster_risk_3_disaster_category as disaster_category,
                disaster_risk_3_rank as disaster_rank,
                COUNT(*) as building_count
            FROM building_optimized
            WHERE disaster_risk_3_disaster_category IS NOT NULL
            GROUP BY disaster_risk_3_disaster_category, disaster_risk_3_rank
            ORDER BY disaster_category, disaster_rank
            """

            result = conn.execute(sql_query).fetchall()
            conn.close()

            if not result:
                return f"❌ {region}の災害データが見つかりませんでした。"

            # 結果を整形
            report = f"📊 {region}の災害種類・ランク別建物数集計\n"
            report += "=" * 60 + "\n"
            report += f"🏢 総建物数: {total_buildings:,}棟\n\n"

            # 災害種類別にグループ化
            disaster_data = {}
            for row in result:
                category, rank, count = row
                if category not in disaster_data:
                    disaster_data[category] = []
                disaster_data[category].append((rank, count))

            # 各災害種類ごとに詳細レポート
            for category, rank_data in disaster_data.items():
                report += f"🚨 {category}:\n"
                report += "-" * 40 + "\n"

                category_total = sum(count for _, count in rank_data)
                category_percentage = (category_total / total_buildings) * 100

                report += f"  総数: {category_total:,}棟 ({category_percentage:.1f}%)\n"
                report += "  ランク別内訳:\n"

                for rank, count in sorted(rank_data):
                    rank_percentage = (count / category_total) * 100 if category_total > 0 else 0
                    report += f"    - ランク{rank}: {count:,}棟 ({rank_percentage:.1f}%)\n"

                report += "\n"

            # 全体統計
            total_disaster_buildings = sum(row[2] for row in result)
            report += f"📈 災害リスク対象建物: {total_disaster_buildings:,}棟 ({(total_disaster_buildings/total_buildings)*100:.1f}%)\n"
            report += f"📈 リスク対象外建物: {total_buildings - total_disaster_buildings:,}棟 ({((total_buildings - total_disaster_buildings)/total_buildings)*100:.1f}%)\n"

            return report
            
        except Exception as e:
            return f"❌ データベース接続エラー: {e}\nDuckDBファイルのパスを確認してください: {HazardFAQData.db_path}"

    def get_disaster_rank_analysis(self, query: str) -> str:
        """災害種別のリスクランク別詳細分析と対策立案"""
        try:
            # DuckDBに接続
            conn = duckdb.connect(HazardFAQData.db_path)
            
            # 地域名を抽出
            region = "大阪市此花区"
            if "此花区" in query:
                region = "大阪市此花区"
            elif "大阪市" in query:
                region = "大阪市"

            # 1. 災害種別・ランク別の詳細集計
            detailed_sql = """
            SELECT
                disaster_risk_1_disaster_category as disaster_category,
                disaster_risk_1_rank as disaster_rank,
                COUNT(*) as building_count,
                AVG(CAST(measuredHeight AS FLOAT)) as avg_height,
                COUNT(CASE WHEN detailedUsage = '住宅' THEN 1 END) as residential_count,
                COUNT(CASE WHEN detailedUsage = '商業' THEN 1 END) as commercial_count,
                COUNT(CASE WHEN detailedUsage = '工場' THEN 1 END) as industrial_count
            FROM building_optimized
            WHERE disaster_risk_1_disaster_category IS NOT NULL
            GROUP BY disaster_risk_1_disaster_category, disaster_risk_1_rank
            UNION ALL
            SELECT
                disaster_risk_2_disaster_category as disaster_category,
                disaster_risk_2_rank as disaster_rank,
                COUNT(*) as building_count,
                AVG(CAST(measuredHeight AS FLOAT)) as avg_height,
                COUNT(CASE WHEN detailedUsage = '住宅' THEN 1 END) as residential_count,
                COUNT(CASE WHEN detailedUsage = '商業' THEN 1 END) as commercial_count,
                COUNT(CASE WHEN detailedUsage = '工場' THEN 1 END) as industrial_count
            FROM building_optimized
            WHERE disaster_risk_2_disaster_category IS NOT NULL
            GROUP BY disaster_risk_2_disaster_category, disaster_risk_2_rank
            UNION ALL
            SELECT
                disaster_risk_3_disaster_category as disaster_category,
                disaster_risk_3_rank as disaster_rank,
                COUNT(*) as building_count,
                AVG(CAST(measuredHeight AS FLOAT)) as avg_height,
                COUNT(CASE WHEN detailedUsage = '住宅' THEN 1 END) as residential_count,
                COUNT(CASE WHEN detailedUsage = '商業' THEN 1 END) as commercial_count,
                COUNT(CASE WHEN detailedUsage = '工場' THEN 1 END) as industrial_count
            FROM building_optimized
            WHERE disaster_risk_3_disaster_category IS NOT NULL
            GROUP BY disaster_risk_3_disaster_category, disaster_risk_3_rank
            ORDER BY disaster_category, disaster_rank
            """

            result = conn.execute(detailed_sql).fetchall()
            conn.close()

            if not result:
                return f"❌ {region}の災害データが見つかりませんでした。"

            # 結果を整形
            report = f"🔍 {region}の災害種別・リスクランク別詳細分析\n"
            report += "=" * 70 + "\n\n"

            # 災害種別にグループ化
            disaster_analysis = {}
            for row in result:
                category, rank, count, avg_height, residential, commercial, industrial = row
                if category not in disaster_analysis:
                    disaster_analysis[category] = []
                disaster_analysis[category].append({
                    'rank': rank,
                    'count': count,
                    'avg_height': avg_height or 0,
                    'residential': residential,
                    'commercial': commercial,
                    'industrial': industrial
                })

            # 各災害種別の詳細分析
            for category, rank_data in disaster_analysis.items():
                report += f"🚨 【{category}】のリスクランク別分析\n"
                report += "=" * 50 + "\n"
                
                total_category_buildings = sum(item['count'] for item in rank_data)
                
                for item in sorted(rank_data, key=lambda x: x['rank']):
                    rank = item['rank']
                    count = item['count']
                    avg_height = item['avg_height']
                    residential = item['residential']
                    commercial = item['commercial']
                    industrial = item['industrial']
                    
                    report += f"\n📊 ランク{rank}:\n"
                    report += f"  • 建物数: {count:,}棟\n"
                    report += f"  • 平均高さ: {avg_height:.1f}m\n"
                    report += f"  • 用途別内訳:\n"
                    report += f"    - 住宅: {residential:,}棟 ({(residential/count)*100:.1f}%)\n"
                    report += f"    - 商業: {commercial:,}棟 ({(commercial/count)*100:.1f}%)\n"
                    report += f"    - 工場: {industrial:,}棟 ({(industrial/count)*100:.1f}%)\n"
                
                report += f"\n📈 {category}の総合統計:\n"
                report += f"  • 総建物数: {total_category_buildings:,}棟\n"
                report += f"  • 平均高さ: {sum(item['avg_height'] * item['count'] for item in rank_data) / total_category_buildings:.1f}m\n"
                
                # 対策立案のための分析（ランクを数値として扱う）
                high_risk_buildings = sum(item['count'] for item in rank_data if self._is_high_risk_rank(item['rank']))
                residential_high_risk = sum(item['residential'] for item in rank_data if self._is_high_risk_rank(item['rank']))
                
                report += f"\n🎯 対策立案のための分析:\n"
                report += f"  • 高リスク建物（ランク3以上）: {high_risk_buildings:,}棟\n"
                report += f"  • 高リスク住宅: {residential_high_risk:,}棟\n"
                
                # 対策案の生成
                report += f"\n💡 推奨対策案:\n"
                if residential_high_risk > 0:
                    report += f"  1. 住宅避難計画の策定（{residential_high_risk:,}棟対象）\n"
                if high_risk_buildings > 0:
                    report += f"  2. 建物耐水性能の向上（{high_risk_buildings:,}棟対象）\n"
                if any(self._is_very_high_risk_rank(item['rank']) for item in rank_data):
                    report += f"  3. 緊急避難場所の整備・拡充\n"
                if any(item['industrial'] > 0 for item in rank_data):
                    report += f"  4. 工場等の防災対策強化\n"
                
                report += "\n" + "="*70 + "\n\n"

            return report
            
        except Exception as e:
            return f"❌ データベース接続エラー: {e}\nDuckDBファイルのパスを確認してください: {HazardFAQData.db_path}"

    def _is_high_risk_rank(self, rank: str) -> bool:
        """ランクが高リスク（3以上）かどうかを判定"""
        try:
            # 数値として解析可能かチェック
            if rank.isdigit():
                return int(rank) >= 3
            # 文字列パターンで判定
            high_risk_patterns = ['3m以上', '5m以上', '10m以上', 'ランク3', 'ランク4', 'ランク5']
            return any(pattern in rank for pattern in high_risk_patterns)
        except:
            return False

    def _is_very_high_risk_rank(self, rank: str) -> bool:
        """ランクが超高リスク（4以上）かどうかを判定"""
        try:
            # 数値として解析可能かチェック
            if rank.isdigit():
                return int(rank) >= 4
            # 文字列パターンで判定
            very_high_risk_patterns = ['5m以上', '10m以上', 'ランク4', 'ランク5']
            return any(pattern in rank for pattern in very_high_risk_patterns)
        except:
            return False

    def generate_countermeasures_with_llm(self, query: str) -> str:
        """DuckDBクエリ結果を基にLLMで対策立案"""
        try:
            # まず災害ランク別分析を実行
            analysis_result = self.get_disaster_rank_analysis(query)
            
            # 分析結果を基にLLMで対策立案
            from langchain_openai import ChatOpenAI
            import os
            from dotenv import load_dotenv
            
            load_dotenv()
            api_key = os.environ.get('OPEN_AI_API_KEY')
            
            if not api_key:
                return f"{analysis_result}\n\n❌ OpenAI APIキーが設定されていません。"
            
            llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3, api_key=api_key)
            
            prompt = f"""
以下の災害リスク分析結果を基に、具体的な対策立案を行ってください。

【分析結果】
{analysis_result}

【要求事項】
1. 各災害種別・ランク別の特徴を踏まえた対策
2. 優先度の高い対策から順番に提示
3. 実現可能性と効果を考慮した具体的な施策
4. 住民向けの説明資料として活用できる内容

【出力形式】
- 対策のタイトル
- 具体的な施策内容
- 対象建物数・世帯数
- 実施期間・費用の目安
- 期待される効果

日本語で回答してください。
"""
            
            response = llm.invoke(prompt)
            return f"{analysis_result}\n\n🤖 AI対策立案:\n{response.content}"
            
        except Exception as e:
            return f"❌ 対策立案エラー: {e}"
