"""
セキュアPostgreSQLツール統合
------------------------------------------------------------
エラーハンドリングとセキュリティ機能を強化した
PostgreSQL地理空間クエリツールの統合版

機能:
- 包括的なエラーハンドリング
- SQLインジェクション対策
- 接続プール管理
- ログ記録
- パフォーマンス監視
"""

from __future__ import annotations
import json
import os
import re
import logging
import time
from typing import Optional, List, Dict, Any, Tuple, Union
from contextlib import contextmanager
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
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

# ============================== ログ設定 ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================== セキュリティ設定 ==============================
FORBIDDEN_KEYWORDS = (
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
    "COPY", "CREATE", "ATTACH", "DETACH", "PRAGMA", "VACUUM",
    "GRANT", "REVOKE", "EXECUTE", "CALL", "DO", "BEGIN", "COMMIT", "ROLLBACK"
)

FORBIDDEN_FUNCTIONS = (
    "pg_sleep", "pg_read_file", "pg_ls_dir", "lo_import", "lo_export",
    "copy", "\\copy", "\\d", "\\dt", "\\l", "\\c"
)

DEFAULT_LIMIT = 1000
MAX_QUERY_TIME = 30  # 秒
MAX_RESULT_ROWS = 10000

# ============================== 接続プール管理 ==============================
class DatabasePool:
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        if self._pool is None:
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=os.getenv("POSTGRES_PORT", "5432"),
                    database=os.getenv("POSTGRES_DB", "plateau"),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", "password")
                )
                logger.info("データベース接続プールを初期化しました")
            except Exception as e:
                logger.error(f"データベース接続プール初期化エラー: {e}")
                raise
        return self._pool
    
    def close_pool(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("データベース接続プールを閉じました")

# ============================== セキュリティユーティリティ ==============================
class SecurityValidator:
    @staticmethod
    def validate_sql(sql: str) -> Tuple[bool, str]:
        """SQLの安全性を検証"""
        sql_upper = sql.upper().strip()
        
        # 禁止キーワードチェック
        for keyword in FORBIDDEN_KEYWORDS:
            if keyword in sql_upper:
                return False, f"禁止キーワード '{keyword}' が検出されました"
        
        # 禁止関数チェック
        for func in FORBIDDEN_FUNCTIONS:
            if func.lower() in sql.lower():
                return False, f"禁止関数 '{func}' が検出されました"
        
        # SELECT文のみ許可
        if not sql_upper.startswith('SELECT'):
            return False, "SELECT文のみ許可されています"
        
        # 複数文の実行を禁止
        if ';' in sql and sql.count(';') > 1:
            return False, "複数文の実行は禁止されています"
        
        return True, "OK"
    
    @staticmethod
    def sanitize_input(user_input: str) -> str:
        """ユーザ入力をサニタイズ"""
        # 基本的なSQLインジェクション対策
        dangerous_patterns = [
            r"'.*'.*'.*",  # シングルクォートの連続
            r"--.*",       # SQLコメント
            r"/\*.*\*/",   # ブロックコメント
            r"union.*select",  # UNION攻撃
            r"or.*1.*=.*1",   # 常に真の条件
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, user_input, re.IGNORECASE):
                logger.warning(f"危険なパターンが検出されました: {pattern}")
                # パターンを除去
                user_input = re.sub(pattern, "", user_input, flags=re.IGNORECASE)
        
        return user_input.strip()

# ============================== パフォーマンス監視 ==============================
class PerformanceMonitor:
    @staticmethod
    @contextmanager
    def monitor_query(sql: str):
        """クエリ実行時間を監視"""
        start_time = time.time()
        try:
            yield
        finally:
            execution_time = time.time() - start_time
            if execution_time > MAX_QUERY_TIME:
                logger.warning(f"クエリ実行時間が長すぎます: {execution_time:.2f}秒")
            logger.info(f"クエリ実行時間: {execution_time:.2f}秒")

# ============================== データベース操作 ==============================
class DatabaseOperations:
    def __init__(self):
        self.pool_manager = DatabasePool()
        self.security = SecurityValidator()
        self.monitor = PerformanceMonitor()
    
    @contextmanager
    def get_connection(self):
        """データベース接続を取得（コンテキストマネージャー）"""
        pool = self.pool_manager.get_pool()
        conn = None
        try:
            conn = pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"データベース接続エラー: {e}")
            raise
        finally:
            if conn:
                pool.putconn(conn)
    
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> Tuple[List[Dict], List[str]]:
        """安全なクエリ実行"""
        # セキュリティ検証
        is_safe, message = self.security.validate_sql(sql)
        if not is_safe:
            raise ValueError(f"SQLセキュリティエラー: {message}")
        
        with self.get_connection() as conn:
            with self.monitor.monitor_query(sql):
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
                        columns = [desc[0] for desc in cur.description] if cur.description else []
                        
                        # 結果行数制限
                        if len(rows) > MAX_RESULT_ROWS:
                            logger.warning(f"結果行数が制限を超えています: {len(rows)} > {MAX_RESULT_ROWS}")
                            rows = rows[:MAX_RESULT_ROWS]
                        
                        return [dict(row) for row in rows], columns
                        
                except psycopg2.Error as e:
                    logger.error(f"PostgreSQLエラー: {e}")
                    raise
                except Exception as e:
                    logger.error(f"クエリ実行エラー: {e}")
                    raise
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """テーブルスキーマを取得"""
        schema_sql = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position;
        """
        
        try:
            rows, columns = self.execute_query(schema_sql, (table_name,))
            return {
                "columns": [{"name": row["column_name"], "type": row["data_type"], 
                           "nullable": row["is_nullable"]} for row in rows],
                "table_name": table_name
            }
        except Exception as e:
            logger.error(f"スキーマ取得エラー: {e}")
            return {"columns": [], "table_name": table_name}

# ============================== 統合セキュアツール ==============================
class SecurePostgresTool(Tool):
    _llm: Any = PrivateAttr(default=None)
    _db_ops: DatabaseOperations = PrivateAttr(default=None)
    
    """
    入力JSON:
      {
        "user_prompt": "自然文の要望",           # 必須
        "spatial_filter": {                     # 任意: 地理的範囲フィルタ
          "type": "bbox|point|polygon",
          "coordinates": [...]
        },
        "max_rows": 1000,                      # 任意
        "as_geojson": false,                   # 任意
        "retries": 2,                          # 任意
        "context": "追加コンテキスト"           # 任意
      }
    返り値(JSON): {
        "result": {"columns":[...],"rows":[...]},
        "sql": "SELECT ...",
        "execution_time": 1.23,
        "security_status": "safe",
        "notes": "..."
      }
    """
    
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        self._db_ops = DatabaseOperations()
        super().__init__(
            name="SecurePostgresTool",
            func=self._run,
            description="セキュアなPostgreSQL地理空間クエリツール。エラーハンドリングとセキュリティ機能を強化。"
        )
    
    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm
    
    def _generate_sql(self, user_prompt: str, context: str = "", max_rows: int = DEFAULT_LIMIT) -> str:
        """LLMを使ってSQLクエリを生成"""
        try:
            # スキーマ情報を取得
            buildings_schema = self._db_ops.get_table_schema("buildings")
            risks_schema = self._db_ops.get_table_schema("disaster_risks")
            
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
                buildings_schema=json.dumps(buildings_schema, ensure_ascii=False),
                risks_schema=json.dumps(risks_schema, ensure_ascii=False),
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
            
        except Exception as e:
            logger.error(f"SQL生成エラー: {e}")
            raise
    
    def _fix_sql(self, user_prompt: str, context: str, prev_sql: str, error: str, max_rows: int) -> str:
        """エラーを踏まえてSQLを修正"""
        try:
            buildings_schema = self._db_ops.get_table_schema("buildings")
            risks_schema = self._db_ops.get_table_schema("disaster_risks")
            
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
                buildings_schema=json.dumps(buildings_schema, ensure_ascii=False),
                risks_schema=json.dumps(risks_schema, ensure_ascii=False),
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
            
        except Exception as e:
            logger.error(f"SQL修正エラー: {e}")
            raise
    
    def _run(self, expression: str) -> str:
        start_time = time.time()
        sql_history: List[Dict[str, Any]] = []
        
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
        
        # 入力サニタイズ
        user_prompt = self._db_ops.security.sanitize_input(user_prompt)
        context = self._db_ops.security.sanitize_input(context)
        
        try:
            # 初回SQL生成
            sql = self._generate_sql(user_prompt, context, max_rows)
            sql_history.append({"sql": sql, "status": "generated", "error": None})
            
            # 初回実行
            try:
                # GeoJSON変換（必要に応じて）
                if as_geojson:
                    sql = re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
                
                rows, columns = self._db_ops.execute_query(sql)
                
                # Decimal型をfloatに変換（JSONシリアライズ対応）
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
                
                execution_time = time.time() - start_time
                
                return json.dumps({
                    "result": {"columns": columns, "rows": processed_rows},
                    "sql": sql,
                    "execution_time": round(execution_time, 3),
                    "security_status": "safe",
                    "sql_history": sql_history,
                    "notes": f"実行成功: {len(processed_rows)}件の結果"
                }, ensure_ascii=False)
                
            except Exception as e:
                sql_history.append({"sql": sql, "status": "error", "error": str(e)})
                last_error = str(e)
                last_sql = sql
                
                # リトライ
                for attempt in range(retries):
                    try:
                        sql = self._fix_sql(user_prompt, context, last_sql, last_error, max_rows)
                        sql_history.append({"sql": sql, "status": "retry_generated", "error": None})
                        
                        # GeoJSON変換（必要に応じて）
                        if as_geojson:
                            sql = re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
                        
                        rows, columns = self._db_ops.execute_query(sql)
                        
                        # Decimal型をfloatに変換
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
                        
                        execution_time = time.time() - start_time
                        sql_history.append({"sql": sql, "status": "ok", "error": None})
                        
                        return json.dumps({
                            "result": {"columns": columns, "rows": processed_rows},
                            "sql": sql,
                            "execution_time": round(execution_time, 3),
                            "security_status": "safe",
                            "sql_history": sql_history,
                            "notes": f"リトライ{attempt + 1}回目で成功: {len(processed_rows)}件の結果"
                        }, ensure_ascii=False)
                        
                    except Exception as retry_error:
                        sql_history.append({"sql": sql, "status": "error", "error": str(retry_error)})
                        last_error = str(retry_error)
                        last_sql = sql
                
                # 全リトライ失敗
                execution_time = time.time() - start_time
                return json.dumps({
                    "error": "Failed after retries",
                    "sql_history": sql_history,
                    "execution_time": round(execution_time, 3),
                    "security_status": "safe"
                }, ensure_ascii=False)
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"ツール実行エラー: {e}")
            return json.dumps({
                "error": str(e),
                "sql_history": sql_history,
                "execution_time": round(execution_time, 3),
                "security_status": "error"
            }, ensure_ascii=False)

# =============================== サンプル実行 ===============================
if __name__ == "__main__":
    # 環境変数確認
    required_env = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        print(f"Missing environment variables: {missing}")
        exit(1)
    
    # セキュアツールのテスト
    secure_tool = SecurePostgresTool()
    result = secure_tool.run(json.dumps({
        "user_prompt": "大阪市内の建物用途別件数を集計して、上位10位まで表示して",
        "context": "大阪市の地理的範囲で分析",
        "max_rows": 10,
        "retries": 2
    }))
    print("[Secure Tool]", result)
    
    # 接続プールのクリーンアップ
    DatabasePool().close_pool()
