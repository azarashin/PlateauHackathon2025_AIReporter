#!/usr/bin/env python
"""
LangChain Tool をクラス継承形式で e-Stat API を呼び出す実装例（集計/地域指定対応版）。

- 札幌市●区のみ取得: ward_codes に区コード（01101〜01110）を指定
- 札幌市（全体=市レベル）の値を取得: city_code=01100 を指定（テーブルに市レベル行があるため自前集計不要）
- 札幌市10区を取得して自前合算: sapporo_all_wards=true と aggregate=true
- 全国の市区町村コードと名称の対応は city_codes.json 等の外部ファイルに保持し、CityCodeResolver ユーティリティで検索できるようにした

依存関係:
- pip install langchain langchain-openai requests python-dotenv pydantic
- 環境変数: E_STAT_APP_ID
"""

import logging
import os
import re
import json
import math
import ast
import requests
from typing import Any, Dict, List, Tuple, Optional
from dotenv import load_dotenv
from pydantic import PrivateAttr

from langchain.agents import Tool

E_STAT_BASE = "http://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
SAPPORO_CITY_CODE = "01100"
SAPPORO_WARD_CODES = [
    "01101", "01102", "01103", "01104", "01105",
    "01106", "01107", "01108", "01109", "01110",
]

# -------------------- CityCodeResolver --------------------
class CityCodeResolver:
    """全国の市区町村コードと名称の対応を検索するユーティリティ。

    別ファイル city_codes.json に {"01100": "札幌市", "01101": "札幌市中央区", ...} の形式で保持する想定。
    """

    def __init__(self, filepath: str = "city_codes.json"):
        self.filepath = filepath
        self.codes: Dict[str, str] = {}
        try:
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.codes = json.load(f)
            else:
                raise FileNotFoundError(f"city_codes.json が見つかりません: {filepath}")
        except Exception as e:
            # ログだけ残し、空のデータで継続
            logging.exception("CityCodeResolver 初期化失敗: %s", e)
            self.codes = {}

    def name_from_code(self, code: str) -> Optional[str]:
        return self.codes.get(code)

    def code_from_name(self, name: str) -> Optional[str]:
        for k, v in self.codes.items():
            if v == name:
                return k
        return None

    def search_by_name(self, keyword: str) -> Dict[str, str]:
        return {k: v for k, v in self.codes.items() if keyword in v}


class EStatTool(Tool):
    """e-Stat getStatsData 用 LangChain Tool（地域指定と合算に対応）。

    入力 JSON 形式（例）:
    {
      "stats_id": "0003445078",
      "params": {"cdTime": "2020000000", "cdCat01": "0"},
      "area": {
        "city_code": "01100",
        "ward_codes": ["01101", "01102"],
        "sapporo_all_wards": true
      },
      "aggregate": true
    }

    返却: 整形済み JSON 文字列。
    """

    _app_id: str = PrivateAttr(default="")

    def __init__(self):
        load_dotenv()
        raw_app_id = os.getenv("E_STAT_APP_ID", "")
        try:
            if not raw_app_id:
                raise RuntimeError("環境変数 E_STAT_APP_ID を設定してください。")
        except Exception as e:
            logging.exception("EStatTool 初期化失敗: %s", e)
            self._app_id = None
            return


        try:
            app_id = json.loads(raw_app_id)
        except Exception:
            app_id = raw_app_id.strip('"')

        super().__init__(
            name="estat_get_stats",
            func=self._estat_query,
            description=(
                "e-Stat の getStatsData を呼び出すツール。入力は JSON。" \
                "必須: stats_id（統計表ID）。任意: params, area(city_code/ward_codes/sapporo_all_wards), aggregate。"
            ),
        )
        self._app_id = app_id

    @staticmethod
    def _ensure_list(x: Any) -> List[Any]:
        if x is None:
            return []
        if isinstance(x, list):
            return x
        return [x]

    @staticmethod
    def _norm(s: str) -> str:
        return (s or "").strip().replace("\u3000", " ").replace(" ", "")

    @staticmethod
    def _load_codes_json(path: str) -> Dict[str, Any]:
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def _resolve_names_to_codes(cls, names: List[str], json_path: str) -> List[str]:
        """city_codes.json を読み、名称→コードを解決（JSONは dict か list を想定）"""
        obj = cls._load_codes_json(json_path)

        # コード→メタ
        code_meta: Dict[str, Dict[str, Any]] = {}
        if isinstance(obj, dict):
            for code, meta in obj.items():
                code_meta[code] = {
                    "name": meta.get("name") or meta.get("city_name") or meta.get("ward_name") or meta.get("pref_name"),
                    "level": int(meta.get("level") or 0),
                    "parent": meta.get("parent_code"),
                    "aliases": meta.get("aliases") or [],
                }
        elif isinstance(obj, list):
            for row in obj:
                code = row.get("area_code") or row.get("code")
                if not code:
                    continue
                code_meta[code] = {
                    "name": row.get("name") or row.get("city_name") or row.get("ward_name") or row.get("pref_name"),
                    "level": int(row.get("level") or 0),
                    "parent": row.get("parent_code"),
                    "aliases": row.get("aliases") or [],
                }
        else:
            return []

        # 名称→コードの逆引きインデックス
        name_index: Dict[str, List[str]] = {}
        for code, meta in code_meta.items():
            nm = meta.get("name") or ""
            for cand in [nm, * (meta.get("aliases") or [])]:
                if not cand:
                    continue
                key = cls._norm(cand)
                name_index.setdefault(key, []).append(code)

        # 区(level=5) は「区名(市名)」の別名も追加（親が JSON にある場合）
        for code, meta in code_meta.items():
            if meta.get("level") == 5 and meta.get("parent") and meta.get("name"):
                parent = code_meta.get(meta["parent"])
                if parent and parent.get("name"):
                    alt = f"{meta['name']}({parent['name']})"
                    key = cls._norm(alt)
                    name_index.setdefault(key, []).append(code)

        # 解決
        out: List[str] = []
        for n in names:
            key = cls._norm(str(n))
            for c in name_index.get(key, []):
                if c not in out:
                    out.append(c)
        return out

    @staticmethod
    def _parse_values(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        vals = (
            payload.get("GET_STATS_DATA", {})
            .get("STATISTICAL_DATA", {})
            .get("DATA_INF", {})
            .get("VALUE", [])
        )
        if isinstance(vals, dict):
            vals = [vals]
        return vals

    @staticmethod
    def _to_number(s: Any) -> float:
        if s is None:
            return math.nan
        if isinstance(s, (int, float)):
            return float(s)
        ss = str(s).replace(",", "")
        try:
            return float(ss)
        except Exception:
            return math.nan

    @staticmethod
    def _group_key(row: Dict[str, Any], ignore_keys: Tuple[str, ...]) -> Tuple:
        return tuple((k, row.get(k)) for k in sorted(row.keys()) if k.startswith("@") and k not in ignore_keys)

    def _estat_query(self, expression: str) -> str:
        try:
            # --- Accept several input shapes robustly ---
            obj: Dict[str, Any]
            if isinstance(expression, dict):
                obj = expression  # type: ignore[assignment]
            else:
                s = str(expression or "").strip()
                # Strip code fences
                if s.startswith("```"):
                    s = s.strip("`\n ")
                    # Remove an optional language hint like ```json
                    s = re.sub(r"^(json|python)\n", "", s, flags=re.IGNORECASE)
                # Unwrap json.dumps({...}) pattern
                m = re.match(r"^json\.dumps\((.*)\)$", s, flags=re.DOTALL)
                if m:
                    s = m.group(1).strip()
                # If wrapped by single/double quotes, strip once
                if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                    s = s[1:-1]
                # Primary: try JSON
                try:
                    obj = json.loads(s)
                except Exception:
                    # Fallback: try Python literal dict
                    try:
                        lit = ast.literal_eval(s)
                        if isinstance(lit, dict):
                            obj = lit
                        else:
                            raise ValueError("Tool input is neither JSON nor a dict literal.")
                    except Exception as ie:
                        raise ValueError(f"ツール入力の解析に失敗しました。JSON文字列で渡してください。先頭120文字: {s[:120]}") from ie

            stats_id = obj.get("stats_id") or obj.get("statsDataId")
            if not stats_id:
                return "入力 JSON に stats_id が必要です。"

            params: Dict[str, Any] = obj.get("params", {}) or {}
            area_cfg: Dict[str, Any] = obj.get("area", {}) or {}

            # --- 名称 → コード解決（city_codes.json を使用） ---
            names = self._ensure_list(area_cfg.get("names"))
            index_json = area_cfg.get("index_json")
            if names and index_json:
                try:
                    name_codes = self._resolve_names_to_codes([str(x) for x in names], index_json)
                    if name_codes:
                        params = {**params, "cdArea": ",".join(sorted(set(name_codes)))}
                except Exception as _e:
                    # 名前解決に失敗しても致命的ではないので続行（必要ならログなど）
                    pass
            
            # Debug print (optional):
            # print("params", params)

            aggregate: bool = bool(obj.get("aggregate", False))

            if "city_code" in area_cfg and area_cfg["city_code"]:
                params = {**params, "cdArea": str(area_cfg["city_code"])}
                aggregate = False
            else:
                ward_codes: List[str] = []
                if area_cfg.get("sapporo_all_wards"):
                    ward_codes = SAPPORO_WARD_CODES.copy()
                ward_codes += [str(c) for c in self._ensure_list(area_cfg.get("ward_codes"))]
                ward_codes = sorted(set(ward_codes))

                if ward_codes:
                    params = {**params, "cdArea": ",".join(ward_codes)}

            q = {"appId": self._app_id, "statsDataId": stats_id}
            if params:
                q.update(params)

            r = requests.get(E_STAT_BASE, params=q, timeout=60)
            r.raise_for_status()
            data = r.json()

            result = data.get("GET_STATS_DATA", {}).get("RESULT", {})
            if result.get("STATUS") not in (0, "0"):
                return json.dumps({"error": result}, ensure_ascii=False, indent=2)

            values = self._parse_values(data)

            if aggregate:
                groups: Dict[Tuple, float] = {}
                samples: Dict[Tuple, Dict[str, Any]] = {}
                for v in values:
                    key = self._group_key(v, ignore_keys=("@area",))
                    groups[key] = groups.get(key, 0.0) + self._to_number(v.get("$"))
                    if key not in samples:
                        samples[key] = v

                aggregated_rows: List[Dict[str, Any]] = []
                for key, total in groups.items():
                    base = samples[key].copy()
                    base["$"] = str(int(total)) if not math.isnan(total) else None
                    base["@area"] = SAPPORO_CITY_CODE
                    aggregated_rows.append(base)

                out = {
                    "mode": "aggregated",
                    "note": "@area を無視して同一キーで合算（札幌市10区等の合計）",
                    "count": len(aggregated_rows),
                    "rows": aggregated_rows[:50],
                    "source_url": r.url,
                }
                return json.dumps(out, ensure_ascii=False, indent=2)

            out = {
                "mode": "raw",
                "count": len(values),
                "rows": values[:100],
                "source_url": r.url,
            }
            return json.dumps(out, ensure_ascii=False, indent=2)

        except Exception as e:
            return f"[e-Stat ツールエラー] {e}"