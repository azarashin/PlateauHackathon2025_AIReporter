#!/usr/bin/env python
"""
LangChain Tool をクラス継承形式で e-Stat API を呼び出す実装例。
ユーザー指定のスタイル（class XXX(Tool): ...）に倣っています。

依存関係:
- pip install langchain langchain-openai requests
- 環境変数: E_STAT_APP_ID
"""

import os
import json
import requests
from typing import Any, Dict
from dotenv import load_dotenv

from langchain.agents import Tool

E_STAT_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"

class EStatTool(Tool):
    def __init__(self):
        super().__init__(
            name="estat_get_stats",
            func=self._estat_query,
            description=(
                "e-Stat の getStatsData を呼び出すツール。" 
                "問い合わせは JSON 形式で受け付ける。" 
                "必須キー: stats_id（統計表ID）。任意キー: params(dict)。"
            )
        )
        load_dotenv()
        self.app_id = os.getenv("E_STAT_APP_ID", "")
        if not self.app_id:
            raise RuntimeError("環境変数 E_STAT_APP_ID を設定してください。")

    def _estat_query(self, expression: str) -> str:
        try:
            obj: Dict[str, Any] = json.loads(expression)
            stats_id = obj.get("stats_id") or obj.get("statsDataId")
            params: Dict[str, Any] = obj.get("params", {})
            if not stats_id:
                return "入力 JSON に stats_id が必要です。"

            q = {"appId": self.app_id, "statsDataId": stats_id}
            if params:
                q.update(params)
            r = requests.get(E_STAT_BASE, params=q, timeout=60)
            r.raise_for_status()
            data = r.json()
            values = (
                data.get("GET_STATS_DATA", {})
                .get("STATISTICAL_DATA", {})
                .get("DATA_INF", {})
                .get("VALUE", [])
            )
            if isinstance(values, dict):
                values = [values]
            preview = values[:5]
            return json.dumps(preview, ensure_ascii=False, indent=2)
        except Exception as e:
            return f"[e-Stat ツールエラー] {e}"
