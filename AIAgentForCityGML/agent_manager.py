import importlib
import inspect
import logging
import os
import pkgutil
from langchain.agents import initialize_agent, Tool
from langchain_openai import ChatOpenAI
import agent_plugins  # パッケージとしてimportしておく
from dotenv import load_dotenv

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

            # モジュール内のクラスを走査して BasePlugin のサブクラスを取得
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, Tool) and obj is not Tool:
                    try:
                        instance = obj(gml_dirs)
                        instances.append(instance)  # インスタンス化
                    except Exception as e:
                        logging.exception(f"プラグイン({module})の追加に失敗しました")

        return instances
    
    def query(self, message):
        return self._agent.run(message)

if __name__ == "__main__":
    agentManager = AgentManager([])
    print(agentManager.query('東京都２３区の建物について分析して日本語のレポートを作成してください。分析結果は次のようなjson形式で出力してください。[{"title":(分析タイトル名), [{"main_text": (説明文段落, null可), "image": (画像へのURL, , null可), "table": (表データ)}]}]'))
