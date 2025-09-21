"""
PostgreSQL地理空間クエリツール
------------------------------------------------------------
PostgreSQLデータベースに接続して地理空間情報の統計処理を行うツール群

機能:
- 地理的範囲でのフィルタリング
- 建物属性の統計処理
- 災害リスク情報の集計
- LLMによるクエリ生成と実行

依存関係:
    pip install psycopg2-binary langchain-core langchain-community langchain-openai

環境変数:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations
import json
import os
import re
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
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

# ============================== 共通ユーティリティ ==============================
FORBIDDEN = (
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
    "COPY", "CREATE", "ATTACH", "DETACH", "PRAGMA", "VACUUM"
)
DEFAULT_LIMIT = 1000

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

def _get_table_schema(conn: psycopg2.extensions.connection, table_name: str) -> Dict[str, Any]:
    """テーブルのスキーマ情報を取得"""
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
    
    with conn.cursor() as cur:
        cur.execute(schema_sql, [table_name])
        columns = cur.fetchall()
    
    # サンプルデータも取得
    sample_sql = f"SELECT * FROM {table_name} LIMIT 3"
    try:
        sample_rows, sample_cols = _execute_query(conn, sample_sql)
    except Exception:
        sample_rows, sample_cols = [], []
    
    return {
        "columns": [{"name": col[0], "type": col[1], "nullable": col[2]} for col in columns],
        "sample_rows": sample_rows,
        "sample_columns": sample_cols
    }

# =============================== 地理空間クエリツール ===============================
class PostgresSpatialQuery(Tool):
    _llm: Any = PrivateAttr(default=None)
    
    """
    入力JSON:
      {
        "user_prompt": "自然文の要望",           # 必須
        "spatial_filter": {                     # 任意: 地理的範囲フィルタ
          "type": "bbox|point|polygon",
          "coordinates": [xmin, ymin, xmax, ymax] | [x, y, radius] | [[x1,y1], [x2,y2], ...]
        },
        "max_rows": 1000,                      # 任意（LIMIT 上限）
        "as_geojson": false                    # 任意: geometry列をGeoJSONに変換
      }
    返り値(JSON): {"columns":[...],"rows":[...],"sql":"...","notes":"..."}
    """
    
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="PostgresSpatialQuery",
            func=self._run,
            description="PostgreSQLデータベースで地理空間情報の統計処理を行う。建物属性の集計、災害リスク分析など。"
        )
    
    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm
    
    def _build_spatial_filter(self, spatial_filter: Dict[str, Any]) -> str:
        """地理的範囲フィルタのSQL条件を構築"""
        if not spatial_filter:
            return ""
        
        filter_type = spatial_filter.get("type", "bbox")
        coords = spatial_filter.get("coordinates", [])
        
        if filter_type == "bbox" and len(coords) == 4:
            xmin, ymin, xmax, ymax = coords
            return f"""
            AND ST_Intersects(
                ST_Transform(geometry, 4326),
                ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326)
            )
            """
        elif filter_type == "point" and len(coords) == 3:
            x, y, radius = coords
            return f"""
            AND ST_DWithin(
                ST_Transform(geometry, 4326),
                ST_SetSRID(ST_MakePoint({x}, {y}), 4326),
                {radius}
            )
            """
        elif filter_type == "polygon" and len(coords) > 2:
            # ポリゴンの座標を文字列に変換
            coords_str = ", ".join([f"{lon} {lat}" for lon, lat in coords])
            return f"""
            AND ST_Intersects(
                ST_Transform(geometry, 4326),
                ST_SetSRID(ST_MakePolygon(ST_GeomFromText('LINESTRING({coords_str})')), 4326)
            )
            """
        
        return ""
    
    def _generate_sql(self, user_prompt: str, spatial_filter: Dict[str, Any], max_rows: int) -> str:
        """LLMを使ってSQLクエリを生成"""
        conn = _get_db_connection()
        try:
            # スキーマ情報を取得
            buildings_schema = _get_table_schema(conn, "buildings")
            risks_schema = _get_table_schema(conn, "disaster_risks")
            
            system_prompt = f"""
            あなたはPostgreSQL+PostGISのSQLアシスタントです。
            安全な読み取り専用のSELECT文のみを生成してください。
            
            利用可能なテーブル:
            1. buildings - 建物情報
            2. disaster_risks - 災害リスク情報
            
            重要な制約:
            - 必ずLIMIT {max_rows}以下
            - 列名は提示されたスキーマからのみ使用
            - 地理空間列はgeometry（EPSG:4979座標系）
            - ST_Transform(geometry, 4326)でWGS84に変換してから空間演算
            - 災害リスク情報はdisaster_risksテーブルからJOIN
            - 統計処理にはGROUP BY、COUNT、SUM、AVG等を使用
            """
            
            spatial_condition = self._build_spatial_filter(spatial_filter)
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("human", 
                 "建物テーブルスキーマ: {buildings_schema}\n"
                 "災害リスクテーブルスキーマ: {risks_schema}\n"
                 "地理的フィルタ条件: {spatial_condition}\n\n"
                 "要望: {user_prompt}\n\n"
                 "これを満たすSELECT文を1本だけ出力してください。")
            ]).format_messages(
                buildings_schema=json.dumps(buildings_schema, ensure_ascii=False),
                risks_schema=json.dumps(risks_schema, ensure_ascii=False),
                spatial_condition=spatial_condition,
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
    
    def _run(self, expression: str) -> str:
        try:
            params = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False)
        
        user_prompt = params.get("user_prompt", "")
        spatial_filter = params.get("spatial_filter", {})
        max_rows = int(params.get("max_rows", DEFAULT_LIMIT))
        as_geojson = bool(params.get("as_geojson", False))
        
        if not user_prompt:
            return json.dumps({"error": "user_prompt is required"}, ensure_ascii=False)
        
        try:
            # SQL生成
            sql = self._generate_sql(user_prompt, spatial_filter, max_rows)
            
            # セキュリティチェック
            if not _is_readonly(sql):
                return json.dumps({"error": "Forbidden SQL keywords detected"}, ensure_ascii=False)
            
            # LIMIT追加
            sql = _cap_limit(sql, max_rows)
            
            # GeoJSON変換（必要に応じて）
            if as_geojson:
                sql = re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
            
            # クエリ実行
            conn = _get_db_connection()
            try:
                rows, columns = _execute_query(conn, sql)
                
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
                
                return json.dumps({
                    "columns": columns,
                    "rows": processed_rows,
                    "sql": sql,
                    "notes": f"実行成功: {len(processed_rows)}件の結果"
                }, ensure_ascii=False)
                
            finally:
                conn.close()
                
        except Exception as e:
            return json.dumps({
                "error": str(e),
                "sql": sql if 'sql' in locals() else None
            }, ensure_ascii=False)

# =============================== 統計処理専用ツール ===============================
class BuildingStatistics(Tool):
    """
    建物統計処理専用ツール
    入力JSON:
      {
        "stat_type": "usage|height|disaster|structure",  # 必須
        "spatial_filter": {...},                          # 任意
        "group_by": "detailed_usage|building_type|class", # 任意
        "max_rows": 1000                                 # 任意
      }
    """
    
    def __init__(self):
        super().__init__(
            name="BuildingStatistics",
            func=self._run,
            description="建物の統計情報を取得（用途別、高さ別、災害リスク別など）"
        )
    
    def _run(self, expression: str) -> str:
        try:
            params = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False)
        
        stat_type = params.get("stat_type", "usage")
        spatial_filter = params.get("spatial_filter", {})
        group_by = params.get("group_by", "")
        max_rows = int(params.get("max_rows", DEFAULT_LIMIT))
        
        conn = _get_db_connection()
        try:
            # 地理的フィルタ条件
            spatial_condition = ""
            if spatial_filter:
                filter_type = spatial_filter.get("type", "bbox")
                coords = spatial_filter.get("coordinates", [])
                
                if filter_type == "bbox" and len(coords) == 4:
                    xmin, ymin, xmax, ymax = coords
                    spatial_condition = f"""
                    AND ST_Intersects(
                        ST_Transform(b.geometry, 4326),
                        ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 4326)
                    )
                    """
            
            # 統計タイプに応じたSQL生成
            if stat_type == "usage":
                group_field = group_by or "detailed_usage"
                sql = f"""
                SELECT 
                    {group_field},
                    COUNT(*) as count,
                    AVG(measured_height) as avg_height,
                    MAX(measured_height) as max_height,
                    MIN(measured_height) as min_height
                FROM buildings b
                WHERE {group_field} IS NOT NULL
                {spatial_condition}
                GROUP BY {group_field}
                ORDER BY count DESC
                LIMIT {max_rows}
                """
            elif stat_type == "disaster":
                sql = f"""
                SELECT 
                    b.detailed_usage,
                    COUNT(*) as building_count,
                    AVG(b.disaster_risk_count) as avg_risk_count,
                    MAX(b.disaster_risk_count) as max_risk_count,
                    SUM(b.disaster_risk_count) as total_risks,
                    AVG(b.max_flood_depth) as avg_max_depth,
                    MAX(b.max_flood_depth) as max_flood_depth
                FROM buildings b
                WHERE b.disaster_risk_count > 0
                {spatial_condition}
                GROUP BY b.detailed_usage
                ORDER BY total_risks DESC
                LIMIT {max_rows}
                """
            elif stat_type == "height":
                sql = f"""
                SELECT 
                    CASE 
                        WHEN measured_height < 10 THEN '低層(10m未満)'
                        WHEN measured_height < 30 THEN '中層(10-30m)'
                        WHEN measured_height < 60 THEN '高層(30-60m)'
                        ELSE '超高層(60m以上)'
                    END as height_category,
                    COUNT(*) as count,
                    AVG(measured_height) as avg_height
                FROM buildings b
                WHERE measured_height IS NOT NULL
                {spatial_condition}
                GROUP BY height_category
                ORDER BY avg_height
                LIMIT {max_rows}
                """
            else:
                return json.dumps({"error": f"Unknown stat_type: {stat_type}"}, ensure_ascii=False)
            
            rows, columns = _execute_query(conn, sql)
            
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
            
            return json.dumps({
                "columns": columns,
                "rows": processed_rows,
                "sql": sql,
                "stat_type": stat_type,
                "notes": f"{stat_type}統計: {len(processed_rows)}件の結果"
            }, ensure_ascii=False)
            
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
        finally:
            conn.close()

# =============================== サンプル実行 ===============================
if __name__ == "__main__":
    # 環境変数確認
    required_env = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        print(f"Missing environment variables: {missing}")
        exit(1)
    
    # 1) 地理空間クエリのテスト
    spatial_query = PostgresSpatialQuery()
    result = spatial_query.run(json.dumps({
        "user_prompt": "大阪市内の建物用途別件数を集計して",
        "spatial_filter": {
            "type": "bbox",
            "coordinates": [135.4, 34.6, 135.6, 34.8]  # 大阪市の大まかな範囲
        },
        "max_rows": 50
    }))
    print("[Spatial Query]", result)
    
    # 2) 統計処理のテスト
    stats = BuildingStatistics()
    result = stats.run(json.dumps({
        "stat_type": "usage",
        "group_by": "detailed_usage",
        "max_rows": 20
    }))
    print("[Statistics]", result)
