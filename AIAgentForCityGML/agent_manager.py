import importlib
import inspect
import logging
import os
import pkgutil
from langchain.agents import initialize_agent, Tool
from langchain_openai import ChatOpenAI
from AIAgentForCityGML.agent_plugins.get_attrib_frequency import GetStringAttributeFrequency
import agent_plugins  # パッケージとしてimportしておく
from dotenv import load_dotenv
import re

class AgentManager:
    def __init__(self, gml_dirs: list[dir]):
        plugins = self._load_plugins(gml_dirs)
        
        load_dotenv(override=True)
        
        API_KEY = os.environ['OPEN_AI_API_KEY']
        AGENT_TYPE = os.environ['AGENT_TYPE']
        AI_MODEL = os.environ['AI_MODEL']
        AI_MAX_ITERATION = int(os.environ['AI_MAX_ITERATION'])
        AI_TEMPERATURE = float(os.environ['AI_TEMPERATURE'])
        
        logging.info(f'### ENVIRONMENT PARAMETERS ###')
        logging.info(f'AGENT_TYPE: {AGENT_TYPE}')
        logging.info(f'AI_MODEL: {AI_MODEL}')
        logging.info(f'AI_MAX_ITERATION: {AI_MAX_ITERATION}')
        logging.info(f'AI_TEMPERATURE: {AI_TEMPERATURE}')

        lim = ChatOpenAI(model=AI_MODEL, temperature=AI_TEMPERATURE, api_key=API_KEY)
        self._agent = initialize_agent(
            plugins, 
            lim, 
            agent=AGENT_TYPE,
            verbose=True, 
            max_iterations=AI_MAX_ITERATION,
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
                if issubclass(obj, Tool) and not self.is_ignored_as_plugin(obj):
                    try:
                        instance = obj(gml_dirs)
                        instances.append(instance)  # インスタンス化
                    except Exception as e:
                        logging.exception(f"プラグイン({module})の追加に失敗しました")

        return instances

    def is_ignored_as_plugin(self, tp: type): 
        ignore_list = [
            # エージェントのベースとなるクラスであるため、これは直接プラグインとして使わない。
            Tool, 
            # これを直接エージェントとして使うのではなく、これをさらに別クラスで派生させてエージェントとして使うため。
            GetStringAttributeFrequency, 
            # 他にあれば追加する
        ]
        for item in ignore_list:
            if tp is item:
                return True
        return False


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
