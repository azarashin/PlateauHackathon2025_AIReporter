"""
LLM統合クエリ生成ツール
------------------------------------------------------------
LLMを使用して自然言語からPostgreSQLクエリを生成し、
地理空間情報の統計処理を自動化するツール

機能:
- 自然言語からSQLクエリの自動生成
- 地理空間条件の自動解釈
- クエリの安全性チェック
- エラー時の自動修正
"""

from __future__ import annotations
import json
import os
import re
from typing import Optional, List, Dict, Any, Tuple
from langchain.tools import Tool
from pydantic import PrivateAttr

from dotenv import load_dotenv, find_dotenv

# .env をどこから実行しても見つけられるようにロード
load_dotenv(find_dotenv(), override=False)

# LLM 実装
try:
    from langchain_openai import ChatOpenAI
    from langchain.prompts import ChatPromptTemplate
except Exception:
    class ChatOpenAI:
        def __init__(self, *args, **kwargs): ...
        def invoke(self, messages):
            class _R: content = "SELECT 1 AS dummy LIMIT 1"
            return _R()
    
    class ChatPromptTemplate:
        @staticmethod
        def from_messages(messages):
            class _T:
                def format_messages(self, **kwargs):
                    return [{"content": "SELECT 1 AS dummy LIMIT 1"}]
            return _T()

# PostgreSQL接続用
import psycopg2
from psycopg2.extras import RealDictCursor

# ============================== 共通ユーティリティ ==============================
FORBIDDEN = (
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
    "COPY", "CREATE", "ATTACH", "DETACH", "PRAGMA", "VACUUM"
)
DEFAULT_LIMIT = 1000

def _get_db_connection() -> psycopg2.extensions.connection:
    """PostgreSQLデータベース接続を取得"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "plateau"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "password")
    )

def _execute_query(conn: psycopg2.extensions.connection, sql: str) -> Tuple[List[Dict], List[str]]:
    """SQLクエリを実行して結果を返す"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return [dict(row) for row in rows], columns

def _get_table_schema(conn: psycopg2.extensions.connection) -> Dict[str, Any]:
    """データベースのスキーマ情報を取得"""
    # buildingsテーブルのスキーマ
    buildings_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'buildings' 
    ORDER BY ordinal_position;
    """
    
    # disaster_risksテーブルのスキーマ
    risks_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'disaster_risks' 
    ORDER BY ordinal_position;
    """
    
    with conn.cursor() as cur:
        cur.execute(buildings_sql)
        buildings_cols = cur.fetchall()
        
        cur.execute(risks_sql)
        risks_cols = cur.fetchall()
    
    # サンプルデータも取得
    try:
        sample_buildings, _ = _execute_query(conn, "SELECT * FROM buildings LIMIT 3")
        sample_risks, _ = _execute_query(conn, "SELECT * FROM disaster_risks LIMIT 3")
    except Exception:
        sample_buildings, sample_risks = [], []
    
    return {
        "buildings": {
            "columns": [{"name": col[0], "type": col[1], "nullable": col[2]} for col in buildings_cols],
            "sample": sample_buildings
        },
        "disaster_risks": {
            "columns": [{"name": col[0], "type": col[1], "nullable": col[2]} for col in risks_cols],
            "sample": sample_risks
        }
    }

def _is_readonly(sql: str) -> bool:
    """SQLが読み取り専用かチェック"""
    up = sql.upper()
    return not any(k in up for k in FORBIDDEN)

def _has_limit(sql: str) -> bool:
    """SQLにLIMIT句があるかチェック"""
    return bool(re.search(r"\bLIMIT\b\s+\d+", sql, flags=re.IGNORECASE))

def _cap_limit(sql: str, cap: int = DEFAULT_LIMIT) -> str:
    """SQLにLIMIT句を追加（既存の場合はそのまま）"""
    return sql if _has_limit(sql) else f"{sql.rstrip().rstrip(';')} LIMIT {cap}"

# =============================== LLM統合クエリ生成 ===============================
class LLMQueryGenerator(Tool):
    _llm: Any = PrivateAttr(default=None)
    
    """
    入力JSON:
      {
        "user_prompt": "自然文の要望",           # 必須
        "context": "追加のコンテキスト情報",     # 任意
        "max_rows": 1000,                      # 任意（LIMIT 上限）
        "retries": 2,                           # 任意（自己修正最大回数）
        "as_geojson": false                    # 任意: geometry列をGeoJSONに変換
      }
    返り値(JSON): {
        "result": {"columns":[...],"rows":[...]},
        "sql": "SELECT ...",
        "sql_history": [{"sql":"...","status":"ok|error","error":null|"..."}],
        "notes": "..."
      }
    """
    
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="LLMQueryGenerator",
            func=self._run,
            description="自然言語からPostgreSQLクエリを生成し、地理空間統計処理を実行する。"
        )
    
    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm
    
    def _generate_sql(self, user_prompt: str, context: str = "", max_rows: int = DEFAULT_LIMIT) -> str:
        """LLMを使ってSQLクエリを生成"""
        conn = _get_db_connection()
        try:
            schema = _get_table_schema(conn)
            
            system_prompt = f"""
            あなたはPostgreSQL+PostGISの専門SQLアシスタントです。
            建物データと災害リスクデータの統計処理に特化したクエリを生成してください。
            
            利用可能なテーブル:
            1. buildings - 建物情報（geometry列はEPSG:4979座標系）
            2. disaster_risks - 災害リスク情報（buildings.idと関連）
            
            重要な制約:
            - 必ずLIMIT {max_rows}以下
            - 読み取り専用のSELECT文のみ
            - 地理空間演算時はST_Transform(geometry, 4326)でWGS84に変換
            - 統計処理にはGROUP BY、COUNT、SUM、AVG等を使用
            - 災害リスク情報はJOINで結合
            - 座標系: geometry列はEPSG:4979、地理的範囲指定時はWGS84(4326)
            
            よくある統計処理パターン:
            - 用途別集計: GROUP BY detailed_usage
            - 高さ別集計: CASE文で高さカテゴリ化
            - 災害リスク分析: disaster_risksテーブルとJOIN
            - 地理的範囲指定: ST_Intersects, ST_DWithin使用
            """
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("human", 
                 "データベーススキーマ:\n"
                 "建物テーブル: {buildings_schema}\n"
                 "災害リスクテーブル: {risks_schema}\n\n"
                 "コンテキスト: {context}\n\n"
                 "要望: {user_prompt}\n\n"
                 "これを満たすSELECT文を1本だけ出力してください。"
                 "SQLのみを出力し、説明は不要です。")
            ]).format_messages(
                buildings_schema=json.dumps(schema["buildings"], ensure_ascii=False),
                risks_schema=json.dumps(schema["disaster_risks"], ensure_ascii=False),
                context=context,
                user_prompt=user_prompt
            )
            
            response = self._ensure_llm().invoke(prompt)
            sql = response.content.strip()
            
            # SQLからコードブロックを除去
            m = re.search(r"```sql\s*(.+?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
            if m:
                sql = m.group(1).strip()
            
            return sql
            
        finally:
            conn.close()
    
    def _fix_sql(self, user_prompt: str, context: str, prev_sql: str, error: str, max_rows: int) -> str:
        """エラーを踏まえてSQLを修正"""
        conn = _get_db_connection()
        try:
            schema = _get_table_schema(conn)
            
            system_prompt = f"""
            前回のSQLがエラーになりました。エラーを分析して修正してください。
            
            制約:
            - 必ずLIMIT {max_rows}以下
            - 読み取り専用のSELECT文のみ
            - 地理空間演算時はST_Transform(geometry, 4326)でWGS84に変換
            - 列名はスキーマに存在するもののみ使用
            """
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("human", 
                 "データベーススキーマ:\n"
                 "建物テーブル: {buildings_schema}\n"
                 "災害リスクテーブル: {risks_schema}\n\n"
                 "エラー: {error}\n"
                 "前回SQL:\n{prev_sql}\n\n"
                 "コンテキスト: {context}\n"
                 "要望: {user_prompt}\n\n"
                 "修正後のSELECT文を1本だけ出力してください。")
            ]).format_messages(
                buildings_schema=json.dumps(schema["buildings"], ensure_ascii=False),
                risks_schema=json.dumps(schema["disaster_risks"], ensure_ascii=False),
                error=error,
                prev_sql=prev_sql,
                context=context,
                user_prompt=user_prompt
            )
            
            response = self._ensure_llm().invoke(prompt)
            sql = response.content.strip()
            
            # SQLからコードブロックを除去
            m = re.search(r"```sql\s*(.+?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
            if m:
                sql = m.group(1).strip()
            
            return sql
            
        finally:
            conn.close()
    
    def _execute_sql(self, sql: str, as_geojson: bool = False) -> Dict[str, Any]:
        """SQLを実行して結果を返す"""
        if not _is_readonly(sql):
            return {"error": "Forbidden SQL keywords detected", "sql": sql}
        
        sql = _cap_limit(sql, DEFAULT_LIMIT)
        
        # GeoJSON変換（必要に応じて）
        if as_geojson:
            sql = re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
        
        conn = _get_db_connection()
        try:
            rows, columns = _execute_query(conn, sql)
            
            # Decimal型をfloatに変換（JSONシリアライズ対応）
            from decimal import Decimal
            def convert_decimal(obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                return obj
            
            processed_rows = []
            for row in rows:
                processed_row = {}
                for key, value in row.items():
                    processed_row[key] = convert_decimal(value)
                processed_rows.append(processed_row)
            
            return {
                "columns": columns,
                "rows": processed_rows,
                "sql_used": sql
            }
            
        except Exception as e:
            return {"error": str(e), "sql": sql}
        finally:
            conn.close()
    
    def _run(self, expression: str) -> str:
        try:
            params = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False)
        
        user_prompt = params.get("user_prompt", "")
        context = params.get("context", "")
        max_rows = int(params.get("max_rows", DEFAULT_LIMIT))
        retries = int(params.get("retries", 2))
        as_geojson = bool(params.get("as_geojson", False))
        
        if not user_prompt:
            return json.dumps({"error": "user_prompt is required"}, ensure_ascii=False)
        
        sql_history: List[Dict[str, Any]] = []
        
        try:
            # 初回SQL生成
            sql = self._generate_sql(user_prompt, context, max_rows)
            sql_history.append({"sql": sql, "status": "generated", "error": None})
            
            # 初回実行
            result = self._execute_sql(sql, as_geojson)
            if "error" not in result:
                sql_history.append({"sql": result["sql_used"], "status": "ok", "error": None})
                return json.dumps({
                    "result": {"columns": result["columns"], "rows": result["rows"]},
                    "sql": result["sql_used"],
                    "sql_history": sql_history,
                    "notes": "初回実行で成功"
                }, ensure_ascii=False)
            else:
                sql_history.append({"sql": result["sql"], "status": "error", "error": result["error"]})
            
            # リトライ
            last_error = sql_history[-1]["error"]
            last_sql = sql_history[-1]["sql"]
            
            for attempt in range(retries):
                try:
                    sql = self._fix_sql(user_prompt, context, last_sql, last_error, max_rows)
                    sql_history.append({"sql": sql, "status": "retry_generated", "error": None})
                    
                    result = self._execute_sql(sql, as_geojson)
                    if "error" not in result:
                        sql_history.append({"sql": result["sql_used"], "status": "ok", "error": None})
                        return json.dumps({
                            "result": {"columns": result["columns"], "rows": result["rows"]},
                            "sql": result["sql_used"],
                            "sql_history": sql_history,
                            "notes": f"リトライ{attempt + 1}回目で成功"
                        }, ensure_ascii=False)
                    else:
                        sql_history.append({"sql": result["sql"], "status": "error", "error": result["error"]})
                        last_error, last_sql = result["error"], result["sql"]
                        
                except Exception as e:
                    sql_history.append({"sql": sql, "status": "error", "error": str(e)})
                    last_error, last_sql = str(e), sql
            
            return json.dumps({
                "error": "Failed after retries",
                "sql_history": sql_history
            }, ensure_ascii=False)
            
        except Exception as e:
            return json.dumps({
                "error": str(e),
                "sql_history": sql_history
            }, ensure_ascii=False)

# =============================== 自然言語応答生成 ===============================
class NaturalLanguageResponse(Tool):
    _llm: Any = PrivateAttr(default=None)
    
    """
    入力JSON:
      {
        "user_prompt": "自然文の要望",           # 必須
        "result": {"columns":[...],"rows":[...]}, # 必須 (クエリ結果)
        "style": "summary|table|narrative"        # 任意
      }
    返り値: 文字列（日本語の最終応答）
    """
    
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="NaturalLanguageResponse",
            func=self._run,
            description="SQL結果をもとに、ユーザ要望へ自然言語で回答を作成。"
        )
    
    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm
    
    def _run(self, expression: str) -> str:
        try:
            params = json.loads(expression)
        except Exception as e:
            return f"JSON解析エラー: {e}"
        
        user_prompt = params.get("user_prompt", "")
        result = params.get("result", {})
        style = params.get("style", "summary")
        
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        
        system_prompt = """
        あなたは地理空間データの専門アナリストです。
        建物データと災害リスク情報の統計結果を、分かりやすく日本語で説明してください。
        
        重要なポイント:
        - 数値は千区切りで表示
        - 件数や上位Nなどを明示
        - 地理的範囲や条件を明確に
        - 災害リスク情報があれば重点的に説明
        - 推測ではなく事実に基づいた説明のみ
        """
        
        template = (
            "ユーザ要望: {user_prompt}\n"
            "結果列: {columns}\n"
            "結果行数: {row_count}\n"
            "先頭行の例: {first_rows}\n"
            "スタイル: {style}\n\n"
            "これに基づき日本語で最適な回答を作ってください。"
        )
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", template)
        ]).format_messages(
            user_prompt=user_prompt,
            columns=json.dumps(columns, ensure_ascii=False),
            row_count=len(rows),
            first_rows=json.dumps(rows[:3] if rows else [], ensure_ascii=False),
            style=style
        )
        
        try:
            response = self._ensure_llm().invoke(prompt)
            return response.content.strip()
        except Exception as e:
            return f"応答生成エラー: {e}"

# =============================== サンプル実行 ===============================
if __name__ == "__main__":
    # 環境変数確認
    required_env = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        print(f"Missing environment variables: {missing}")
        exit(1)
    
    # 1) LLM統合クエリ生成のテスト
    llm_query = LLMQueryGenerator()
    result = llm_query.run(json.dumps({
        "user_prompt": "大阪市内の建物用途別件数を集計して、上位10位まで表示して",
        "context": "大阪市の地理的範囲で分析",
        "max_rows": 10
    }))
    print("[LLM Query]", result)
    
    # 2) 自然言語応答のテスト
    if "result" in json.loads(result):
        response_gen = NaturalLanguageResponse()
        answer = response_gen.run(json.dumps({
            "user_prompt": "大阪市内の建物用途別件数を集計して",
            "result": json.loads(result)["result"],
            "style": "summary"
        }))
        print("[Natural Response]", answer)
