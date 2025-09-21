import importlib
import inspect
import logging
import os
import pkgutil
import json
from langchain.agents import initialize_agent, Tool
from langchain_openai import ChatOpenAI
import agent_plugins  # パッケージとしてimportしておく
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AgentManager:
    def __init__(self, gml_dirs: list[dir]):
        plugins = self._load_plugins(gml_dirs)
        
        load_dotenv()
        
        API_KEY = os.environ['OPEN_AI_API_KEY']
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

        # plugins パッケージ内の全モジュールを列挙
        for _, module_name, is_pkg in pkgutil.iter_modules(agent_plugins.__path__):
            if is_pkg:
                continue
            full_name = f"{agent_plugins.__name__}.{module_name}"
            module = importlib.import_module(full_name)

            # モジュール内のクラスを走査して Tool のサブクラスを取得
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, Tool) and obj is not Tool:
                    try:
                        # PostgreSQLツールの場合はgml_dirsパラメータを渡さない
                        if 'Postgres' in name or 'LLM' in name or 'Secure' in name:
                            instance = obj()
                        else:
                            instance = obj(gml_dirs)
                        instances.append(instance)  # インスタンス化
                        logger.info(f"プラグイン({name})を正常に読み込みました")
                    except Exception as e:
                        logger.exception(f"プラグイン({name})の追加に失敗しました: {e}")

        return instances
    
    def query(self, message):
        """
        クエリを実行し、エラー時は修正を試行する
        """
        try:
            result = self._agent.run(message)
            return result
        except Exception as e:
            logger.error(f"クエリ実行エラー: {e}")
            # エラー時の修正を試行
            return self._handle_query_error(message, str(e))
    
    def _handle_query_error(self, original_message: str, error_message: str):
        """
        クエリエラー時の修正処理
        """
        try:
            # PostgreSQL関連のエラーの場合の特別処理
            if any(keyword in error_message.lower() for keyword in ['postgres', 'sql', 'database', 'connection']):
                corrected_message = f"""
                データベースクエリでエラーが発生しました。以下の点を確認して修正してください:
                
                元のクエリ: {original_message}
                エラー内容: {error_message}
                
                修正のポイント:
                1. テーブル名や列名が正しいか確認
                2. 地理空間関数の座標系が正しいか確認
                3. データベース接続が正常か確認
                4. より簡単なクエリから段階的に実行
                
                修正されたクエリを実行してください。
                """
            else:
                corrected_message = f"""
                以下のエラーが発生したため、クエリを修正してください:
                
                元のクエリ: {original_message}
                エラー内容: {error_message}
                
                修正されたクエリを実行してください。
                """
            
            # 修正されたクエリを実行
            result = self._agent.run(corrected_message)
            return result
            
        except Exception as retry_error:
            logger.error(f"クエリ修正も失敗しました: {retry_error}")
            return {
                "error": "クエリの実行と修正の両方に失敗しました",
                "original_error": error_message,
                "retry_error": str(retry_error),
                "suggestion": "より具体的な条件を指定するか、データベースの状態を確認してください。",
                "troubleshooting": {
                    "database_connection": "データベース接続を確認してください",
                    "table_schema": "テーブルスキーマを確認してください",
                    "spatial_functions": "地理空間関数の座標系を確認してください",
                    "query_syntax": "SQLクエリの構文を確認してください"
                }
            }

if __name__ == "__main__":
    agentManager = AgentManager([])
    print(agentManager.query('東京都２３区の建物について分析して日本語のレポートを作成してください。分析結果は次のようなjson形式で出力してください。[{"title":(分析タイトル名), [{"main_text": (説明文段落, null可), "image": (画像へのURL, , null可), "table": (表データ)}]}]'))
