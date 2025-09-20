"""
DuckDB × LangChain（Pythonスクリプト前提）
------------------------------------------------------------
前提: Python だけで使う。GUIは不要。
- データは GeoJSON/Parquet/.duckdb のいずれか。
- 1ファイルの .duckdb を他PCへコピーしても、pip の duckdb が入っていればそのまま使える。
- 空間関数を使う場合は各PCで `INSTALL spatial; LOAD spatial;` を実行する。

依存関係（例）:
    pip install duckdb langchain-core langchain-community langchain-openai

※ OpenAI を使う場合は環境変数 OPENAI_API_KEY を設定してください。

構成（Tool クラス）:
  ① LoadSpatialDataset     : GeoJSON/Parquet/.duckdb を読み込み、DuckDB上に view/table として公開
  ② ProposeSQL             : LLM が安全な SELECT を提案（LIMIT 付与・禁止句回避）
  ③ RunSQL                 : SQL を実行して結果を返す（SELECTのみ）
  ④ ComposeAnswer          : 実行結果を自然言語で要約/整形
  ⑤ RunSQLSmart            : ②+③ を自動ループ（エラーを LLM に渡して修正→再実行）

サンプル実行は最下部の `if __name__ == "__main__":` を参照。
"""

from __future__ import annotations
import json, os, pathlib, re
from typing import Optional, List, Dict, Any

import duckdb
from langchain.tools import Tool

from dotenv import load_dotenv, find_dotenv
from pydantic import PrivateAttr

# .env をどこから実行しても見つけられるようにロード
load_dotenv(find_dotenv(), override=False)
# SDK が期待する OPENAI_API_KEY へ、ユーザ既存の OPEN_AI_API_KEY をエイリアス
if os.getenv("OPENAI_API_KEY") is None and os.getenv("OPEN_AI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = os.getenv("OPEN_AI_API_KEY")

# 任意の LLM 実装に差し替え可能（本サンプルは OpenAI）
try:
    from langchain_openai import ChatOpenAI
except Exception:  # OpenAI を使わない/使えない場合のフォールバックダミー
    class ChatOpenAI:  # type: ignore
        def __init__(self, *args, **kwargs): ...
        def invoke(self, messages):
            class _R: content = "SELECT 1 AS dummy LIMIT 1"  # ダミー返答
            return _R()

from langchain.prompts import ChatPromptTemplate

# ============================== 共通ユーティリティ ==============================
FORBIDDEN = (
    "DROP","DELETE","UPDATE","INSERT","ALTER","TRUNCATE",
    "COPY","CREATE","ATTACH","DETACH","PRAGMA","VACUUM"
)
DEFAULT_LIMIT = 500


def _norm(p: str) -> str:
    return str(pathlib.Path(p).expanduser().resolve())


def _connect(db_path: str, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path, read_only=read_only)
    # 空間関数を使う可能性があるのでロードを試みる（オフライン等で失敗しても続行）
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
    except Exception:
        pass
    return con


def _spatial_loaded(con: duckdb.DuckDBPyConnection) -> bool:
    """DuckDBのspatial拡張がロード済みかを判定（失敗時はFalse）。"""
    try:
        row = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'spatial'"
        ).fetchone()
        if not row:
            return False
        val = row[0]
        return bool(val)
    except Exception:
        # 互換のないDuckDBでも安全にFalse
        return False


def _spatial_functions_available(con: duckdb.DuckDBPyConnection) -> bool:
    """ST_* 関数が実際に呼べるかを軽く検査（古いビルドや未ロード対策）。"""
    try:
        con.execute("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))")
        return True
    except Exception:
        return False


def _has_limit(sql: str) -> bool:
    return bool(re.search(r"\bLIMIT\b\s+\d+", sql, flags=re.IGNORECASE))


def _is_readonly(sql: str) -> bool:
    up = sql.upper()
    return not any(k in up for k in FORBIDDEN)


def _cap_limit(sql: str, cap: int = DEFAULT_LIMIT) -> str:
    return sql if _has_limit(sql) else f"{sql.rstrip().rstrip(';')} LIMIT {cap}"


def _relation_preview(con: duckdb.DuckDBPyConnection, relation: str) -> Dict[str, Any]:
    # DESCRIBE を使って安定的に列名/型を取得
    desc_rows = con.execute(f"DESCRIBE {relation}").fetchall()
    # DuckDB の DESCRIBE は [column_name, column_type, null, key, default, extra]
    colinfo = [{"name": r[0], "type": r[1]} for r in desc_rows]

    # 可能なら GEOMETRY 列を GeoJSON に変換してサンプルを取得（JSON安全化）
    spatial_ok = _spatial_loaded(con) and _spatial_functions_available(con)

    def _i(name: str) -> str:
        # 識別子のクオート（大文字小文字/記号対策）
        esc = name.replace('"', '""')
        return f'"{esc}"'

    if colinfo:
        select_cols = []
        for ci in colinfo:
            cname = ci["name"]
            ctype = (ci["type"] or "").upper()
            if spatial_ok and (ctype == "GEOMETRY" or "GEOMETRY" in ctype):
                # そのままの列名で GeoJSON 文字列に変換
                select_cols.append(f"ST_AsGeoJSON({_i(cname)}) AS {_i(cname)}")
            else:
                select_cols.append(_i(cname))
        select_expr = ", ".join(select_cols)
        sample = con.execute(f"SELECT {select_expr} FROM {relation} LIMIT 3").fetchall()
    else:
        sample = []

    # bytes など JSON 化できない値を安全に文字列化
    def _json_safe(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                # 文字列として解釈できる場合は UTF-8 で
                return v.decode("utf-8")
            except Exception:
                # 難しい場合はサイズだけ示す
                return f"<BLOB {len(v)} bytes>"
        return v

    safe_rows: List[List[Any]] = [[_json_safe(x) for x in row] for row in sample]
    return {"columns": colinfo, "sample_rows": safe_rows}


def _drop_any(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """同名の VIEW/TABLE どちらでも安全に削除する（順序: VIEW→TABLE）。"""
    for stmt in (f"DROP VIEW IF EXISTS {name};", f"DROP TABLE IF EXISTS {name};"):
        try:
            con.execute(stmt)
        except Exception:
            # 型不一致などは無視して次へ
            pass


# =============================== ① ローダ ===============================
class LoadSpatialDataset(Tool):
    """
    入力JSON:
      {
        "source_type": "geojson|parquet|geoparquet|duckdb",  # 必須
        "path": "data/source.geojson",                       # 必須（duckdb の場合は .duckdb ファイル）
        "db_path": "geo.duckdb",                             # 任意(既定: geo.duckdb)
        "relation": "places",                                 # 任意(既定: places)
        "mode": "view|table",                                # 任意(既定: view)
        "srid": 4326,                                        # 任意
        "add_bbox_columns": true                             # 任意(既定 true; view/table に bbox/minx..maxy)
      }
    返り値(JSON):
      {"db_path":"...","relation":"...","kind":"view|table","rows":12345}
    """
    def __init__(self):
        super().__init__(
            name="LoadSpatialDataset",
            func=self._run,
            description="GeoJSON/Parquet/.duckdb を読み込み、DuckDB 上に view/table として公開する。"
        )

    def _run(self, expression: str) -> str:
        p = json.loads(expression)
        source_type = p["source_type"].lower()
        path = _norm(p["path"]) if source_type != "duckdb" else p["path"]
        db_path = p.get("db_path", "geo.duckdb")
        relation = p.get("relation", "places")
        mode = p.get("mode", "view").lower()
        srid = int(p.get("srid", 4326))
        add_bbox = bool(p.get("add_bbox_columns", True))

        if source_type != "duckdb" and not os.path.exists(path):
            return json.dumps({"error": f"file not found: {path}"}, ensure_ascii=False)

        # 接続（書き込み可）
        con = _connect(db_path, read_only=False)
        try:
            spatial_ok = _spatial_loaded(con) and _spatial_functions_available(con)

            # 既存の同名オブジェクトを安全に掃除（型不一致でもエラーにしない）
            _drop_any(con, relation)
            _drop_any(con, f"{relation}_base")
            _drop_any(con, f"{relation}_sr")

            # SQL の中で使うパスは ' をエスケープ
            def _q(s: str) -> str:
                return s.replace("'", "''")

            if source_type == "geojson":
                if not spatial_ok:
                    return json.dumps({
                        "error": "DuckDB spatial拡張が未ロードのためGeoJSONを読み込めません。ネット接続可なら拡張の自動取得を有効にするか、DuckDBを最新版に更新してください。"
                    }, ensure_ascii=False)
                base_sql = f"SELECT * FROM ST_Read('{_q(path)}')"
            elif source_type in ("parquet", "geoparquet"):
                base_sql = f"SELECT * FROM read_parquet('{_q(path)}')"
            elif source_type == "duckdb":
                # 既存 .duckdb のテーブルを view 化する（最初のユーザテーブルを自動選択 or relation 指定に合わせる）
                ext_path = _norm(path)
                # 既に extdb があると ATTACH が失敗するため、先に外してから付け直す
                try:
                    con.execute("DETACH DATABASE IF EXISTS extdb;")
                except Exception:
                    pass
                con.execute(f"ATTACH '{_q(ext_path)}' AS extdb (READ_ONLY);")
                # relation が extdb に存在するならそれを使う（任意スキーマ対応）、なければ extdb の先頭テーブル
                has_rel = con.execute(
                    "SELECT table_schema FROM information_schema.tables WHERE table_catalog='extdb' AND table_name=?",
                    [relation]
                ).fetchone()
                if has_rel:
                    schema = has_rel[0]
                    src_rel = f"extdb.{schema}.{relation}"
                else:
                    rows = con.execute(
                        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog='extdb' ORDER BY table_schema, table_name LIMIT 1"
                    ).fetchall()
                    if not rows:
                        return json.dumps({"error": f"no tables in {ext_path}"}, ensure_ascii=False)
                    schema, tname = rows[0]
                    src_rel = f"extdb.{schema}.{tname}"
                base_sql = f"SELECT * FROM {src_rel}"
            else:
                return json.dumps({"error": "unknown source_type"}, ensure_ascii=False)

            if mode == "view":
                # extdb など外部DB参照を残さないよう、まずローカルに基底テーブルを作成
                con.execute(f"CREATE TABLE {relation}_base AS {base_sql};")
                # extdb を使っていた場合はこの時点で切り離しても良い
                if source_type == "duckdb":
                    try:
                        con.execute("DETACH DATABASE IF EXISTS extdb;")
                    except Exception:
                        pass
                if spatial_ok:
                    try:
                        con.execute(
                            f"""
                            CREATE OR REPLACE VIEW {relation}_sr AS
                            SELECT CASE WHEN ST_SRID(geometry) IS NULL
                                        THEN ST_SetSRID(geometry, {srid}) ELSE geometry END AS geometry,
                                    *
                            FROM {relation}_base;
                            """
                        )
                        final_rel = f"{relation}_sr"
                        if add_bbox:
                            con.execute(
                                f"""
                                CREATE OR REPLACE VIEW {relation} AS
                                SELECT
                                    ST_Envelope(geometry) AS bbox,
                                    ST_XMin(ST_Envelope(geometry)) AS minx,
                                    ST_YMin(ST_Envelope(geometry)) AS miny,
                                    ST_XMax(ST_Envelope(geometry)) AS maxx,
                                    ST_YMax(ST_Envelope(geometry)) AS maxy,
                                    *
                                FROM {relation}_sr;
                                """
                            )
                            final_rel = relation
                    except Exception:
                        # 空間関数が利用できない/geometryが無い場合は非空間ビューにフォールバック
                        con.execute(f"CREATE OR REPLACE VIEW {relation} AS SELECT * FROM {relation}_base;")
                        final_rel = relation
                else:
                    # 空間拡張なし: そのまま base を公開（bbox列等は付与しない）
                    con.execute(f"CREATE OR REPLACE VIEW {relation} AS SELECT * FROM {relation}_base;")
                    final_rel = relation
                rows = con.execute(f"SELECT COUNT(*) FROM {final_rel}").fetchone()[0]
                kind = "view"
            else:
                con.execute(f"CREATE TABLE {relation} AS {base_sql};")
                if spatial_ok:
                    try:
                        con.execute(
                            f"UPDATE {relation} SET geometry = ST_SetSRID(geometry, {srid}) WHERE ST_SRID(geometry) IS NULL;"
                        )
                        if add_bbox:
                            con.execute(f"ALTER TABLE {relation} ADD COLUMN IF NOT EXISTS bbox GEOMETRY;")
                            con.execute(
                                f"""
                                ALTER TABLE {relation}
                                ADD COLUMN IF NOT EXISTS minx DOUBLE,
                                ADD COLUMN IF NOT EXISTS miny DOUBLE,
                                ADD COLUMN IF NOT EXISTS maxx DOUBLE,
                                ADD COLUMN IF NOT EXISTS maxy DOUBLE;
                                """
                            )
                            con.execute(f"UPDATE {relation} SET bbox = ST_Envelope(geometry);")
                            con.execute(
                                f"""
                                UPDATE {relation}
                                SET minx = ST_XMin(bbox), miny = ST_YMin(bbox),
                                    maxx = ST_XMax(bbox), maxy = ST_YMax(bbox);
                                """
                            )
                    except Exception:
                        # 非空間DuckDBでも致命的にしない
                        pass
                con.execute("ANALYZE;")
                con.execute("VACUUM;")
                rows = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
                kind = "table"

            return json.dumps({"db_path": db_path, "relation": relation, "kind": kind, "rows": int(rows)}, ensure_ascii=False)
        finally:
            con.close()


# =============================== ② SQL 案出し ===============================
class ProposeSQL(Tool):
    _llm: Any = PrivateAttr(default=None)
    """
    入力JSON:
      {
        "db_path":"geo.duckdb",         # 必須
        "relation":"places",            # 必須
        "user_prompt":"xxx を集計",     # 必須（自然文）
        "max_rows": 500                   # 任意（LIMIT 上限）
      }
    返り値(JSON): {"sql":"SELECT ... LIMIT ...","notes":"..."}
    """
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="ProposeSQL",
            func=self._run,
            description="スキーマと要望から安全な SELECT を提案（LIMIT 付与・禁止句回避）。"
        )

    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm

    def _run(self, expression: str) -> str:
        p = json.loads(expression)
        db_path = p["db_path"]
        relation = p["relation"]
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        user_prompt = p["user_prompt"]

        con = _connect(db_path)
        meta = _relation_preview(con, relation)
        con.close()

        system = (
            "あなたはDuckDB+SpatialのSQLアシスタントです。"
            "安全な読み取り専用SELECT文のみを生成します。DDL/DMLは禁止。"
            f"結果行数は必ず LIMIT {max_rows} 以下に抑えてください。"
            "列名は提示された一覧の中から厳密に選び、存在しない列名は使わないこと。"
            "空間列の名前は geometry です。他の名前(geomなど)を使わないこと。"
            "bbox/minx/miny/maxx/maxy 列があれば粗い矩形フィルタの後に ST_Intersects を使います。"
            "ST_MakeEnvelope は 4引数 (xmin, ymin, xmax, ymax) のみを使用し、SRID 引数は付けないでください。必要なら DOUBLE に明示キャストしてください。"
            "ユーザが『X別に集計』と依頼した場合、X 列で GROUP BY してください（例: detailedUsage 別→ detailedUsage で GROUP BY）。"
            "不要なフィルタ条件（type='park' など）はユーザが依頼しない限り追加しないこと。"
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", system),
            ("human", "利用可能なテーブルは {relation} のみ。列情報: {columns}\nサンプル: {sample}\n\n要望: {ask}\n安全なSQLを1本だけ、理由の説明付きで出してください。")
        ])
        msg = prompt.format_messages(
            relation=relation,
            columns=json.dumps(meta["columns"], ensure_ascii=False),
            sample=json.dumps(meta["sample_rows"], ensure_ascii=False),
            ask=user_prompt,
        )
        resp = self._ensure_llm().invoke(msg)
        txt = resp.content.strip()

        m = re.search(r"```sql\s*(.+?)\s*```", txt, flags=re.IGNORECASE | re.DOTALL)
        sql = m.group(1) if m else txt.split("\n")[0].strip()

        if not _is_readonly(sql):
            return json.dumps({"error": "forbidden keyword detected", "sql": sql}, ensure_ascii=False)
        sql = _cap_limit(sql, max_rows)
        return json.dumps({"sql": sql, "notes": "LLM生成SQL（READONLY/LIMIT済み）"}, ensure_ascii=False)


# =============================== ③ 実行 ===============================
class RunSQL(Tool):
    """
    入力JSON:
      {
        "db_path":"geo.duckdb",           # 必須
        "sql":"SELECT ...",               # 必須 (SELECT のみ)
        "as_geojson": false                 # 任意: geometry列をGeoJSONに変換
      }
    返り値(JSON): {"columns":[...],"rows":[...]}
    """
    def __init__(self):
        super().__init__(
            name="RunSQL",
            func=self._run,
            description="DuckDB で SELECT を実行（禁止句チェックと LIMIT 強制）。"
        )

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
            
        db_path = p["db_path"]
        sql = p["sql"].strip().rstrip(";")
        as_geojson = bool(p.get("as_geojson", False))

        if not _is_readonly(sql):
            return json.dumps({"error": "forbidden keyword detected"}, ensure_ascii=False)
        sql = _cap_limit(sql, DEFAULT_LIMIT)

        con = _connect(db_path)
        # 空間拡張が有効な場合のみ GeoJSON 変換を適用
        spatial_ok = _spatial_loaded(con) and _spatial_functions_available(con)
        sql_mod = (
            re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
            if as_geojson and spatial_ok else sql
        )
        try:
            cur = con.execute(sql_mod)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            out = {"columns": cols, "rows": rows}
        except Exception as e:
            out = {"error": str(e), "sql": sql_mod}
        finally:
            con.close()
        return json.dumps(out, ensure_ascii=False)


# =============================== ④ 応答生成 ===============================
class ComposeAnswer(Tool):
    _llm: Any = PrivateAttr(default=None)
    """
    入力JSON:
      {
        "user_prompt":"自然文の依頼",               # 必須
        "result":{"columns":[...],"rows":[...]},  # 必須 (RunSQL の結果)
        "style":"summary|table|narrative"          # 任意
      }
    返り値: 文字列（日本語の最終応答）
    """
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="ComposeAnswer",
            func=self._run,
            description="SQL 結果をもとに、ユーザ要望へ自然言語で回答を作成。"
        )

    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm

    def _run(self, expression: str) -> str:
        p = json.loads(expression)
        user_prompt = p["user_prompt"]
        result = p["result"]
        style = p.get("style", "summary")

        cols = result.get("columns", [])
        rows = result.get("rows", [])

        sys = (
            "あなたはデータアナリストです。事実に忠実に簡潔に答え、数値は千区切り、"
            "件数や上位Nなどを明示します。SQL結果にない推測はしません。"
        )
        template = (
            "ユーザ要望: {ask}\n"
            "列: {cols}\n"
            "行数: {nrows}\n"
            "先頭行の例: {first}\n"
            "スタイル: {style}\n"
            "これに基づき日本語で最適な回答を作ってください。"
        )
        msg = ChatPromptTemplate.from_messages([("system", sys), ("human", template)]).format_messages(
            ask=user_prompt,
            cols=json.dumps(cols, ensure_ascii=False),
            nrows=len(rows),
            first=json.dumps(rows[0] if rows else [], ensure_ascii=False),
            style=style,
        )
        return self._ensure_llm().invoke(msg).content.strip()


# ===================== ②+③ 自己修正ループ付き実行 =====================
class RunSQLSmart(Tool):
    _llm: Any = PrivateAttr(default=None)
    """
    入力JSON:
      {
        "db_path":"geo.duckdb",            # 必須
        "relation":"places",               # 必須
        "user_prompt":"自然文の要望",       # 必須
        "max_rows": 500,                     # 任意（LIMIT 上限）
        "retries": 2,                        # 任意（自己修正最大回数）
        "as_geojson": false                  # 任意: GeoJSON 変換
      }
    返り値(JSON): {
        "result":{"columns":[...],"rows":[...]},
        "sql_history":[{"sql":"...","status":"ok|error","error":null|"..."}],
        "notes":"..."
      }
    """
    def __init__(self, llm: Optional[ChatOpenAI] = None):
        api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
        self._llm = llm or ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        super().__init__(
            name="RunSQLSmart",
            func=self._run,
            description="自然文→安全SQL→実行。失敗時はエラーを渡してSQLを自動修正し再実行する。"
        )

    def _ensure_llm(self):
        if self._llm is None:
            api_key = os.getenv("OPEN_AI_API_KEY") or os.getenv("OPENAI_API_KEY")
            self._llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
        return self._llm

    def _render_system(self, max_rows: int) -> str:
        return (
            "あなたはDuckDB+SpatialのSQLアシスタントです。"
            "安全な読み取り専用のSELECT文のみを1本だけ出力してください。DDL/DMLは禁止。\n"
            f"- 必ず LIMIT {max_rows} 以下。\n"
            "- 列名は提示された一覧からのみ使用（存在しない列名は使わない）。空間列の正式名は geometry。\n"
            "- bbox/minx/miny/maxx/maxy があれば矩形フィルタ後に ST_Intersects を使用。\n"
            "- ST_MakeEnvelope は (xmin, ymin, xmax, ymax) の4引数。必要なら DOUBLE にキャスト。SRID 引数は付けない。\n"
            "- 『X別に集計』の要望には X 列で GROUP BY。\n"
            "- 依頼されていないフィルタ条件を勝手に追加しない。\n"
            "- DuckDB の関数/構文に合わせること。\n"
            "- 回答はSQLのみ（説明やコードブロックは不要）。"
        )

    def _first_prompt(self, relation: str, schema: Dict[str, Any], user_prompt: str):
        return ChatPromptTemplate.from_messages([
            ("system", self._render_system(max_rows=DEFAULT_LIMIT)),
            ("human",
             "利用可能なテーブルは {relation} のみです。\n"
             "列情報: {columns}\nサンプル行: {sample}\n\n"
             "要望: {ask}\n"
             "これを満たす SELECT 文を1本だけ出してください。")
        ]).format_messages(
            relation=relation,
            columns=json.dumps(schema["columns"], ensure_ascii=False),
            sample=json.dumps(schema["sample_rows"], ensure_ascii=False),
            ask=user_prompt,
        )

    def _fix_prompt(self, relation: str, schema: Dict[str, Any], user_prompt: str, prev_sql: str, error: str, max_rows: int):
        return ChatPromptTemplate.from_messages([
            ("system", self._render_system(max_rows=max_rows)),
            ("human",
             "前回のSQLがエラーになりました。エラーを踏まえて修正してください。\n"
             "テーブル: {relation}\n列情報: {columns}\nエラー: {error}\n"
             "前回SQL:\n{prev_sql}\n\n要望: {ask}\n"
             "修正後のSELECT文を1本だけ出してください。")
        ]).format_messages(
            relation=relation,
            columns=json.dumps(schema["columns"], ensure_ascii=False),
            error=error,
            prev_sql=prev_sql,
            ask=user_prompt,
        )

    def _extract_sql_only(self, text: str) -> str:
        m = re.search(r"```sql\s*(.+?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        sql = m.group(1).strip() if m else text.strip()
        m2 = re.search(r"(SELECT\b[\s\S]+)", sql, flags=re.IGNORECASE)
        return m2.group(1).strip() if m2 else sql

    def _try_execute(self, db_path: str, sql: str, as_geojson: bool) -> Dict[str, Any]:
        if not _is_readonly(sql):
            return {"error": "forbidden keyword detected in SQL", "sql": sql}
        sql = _cap_limit(sql, DEFAULT_LIMIT)
        con = _connect(db_path)
        # 空間拡張が有効な場合のみ GeoJSON 変換を適用
        spatial_ok = _spatial_loaded(con) and _spatial_functions_available(con)
        sql_mod = (
            re.sub(r"\bgeometry\b", "ST_AsGeoJSON(geometry) AS geometry_geojson", sql, flags=re.IGNORECASE)
            if as_geojson and spatial_ok else sql
        )
        try:
            cur = con.execute(sql_mod)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return {"columns": cols, "rows": rows, "sql_used": sql_mod}
        except Exception as e:
            return {"error": str(e), "sql": sql_mod}
        finally:
            con.close()

    def _run(self, expression: str) -> str:
        p = json.loads(expression)
        db_path = p["db_path"]
        relation = p["relation"]
        user_prompt = p["user_prompt"]
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        retries = int(p.get("retries", 2))
        as_geojson = bool(p.get("as_geojson", False))

        con = _connect(db_path)
        schema = _relation_preview(con, relation)
        con.close()

        sql_history: List[Dict[str, Any]] = []

        # 初回生成
        first = self._first_prompt(relation, schema, user_prompt)
        sql = self._extract_sql_only(self._ensure_llm().invoke(first).content)

        if not _is_readonly(sql):
            sql_history.append({"sql": sql, "status": "error", "error": "forbidden keyword"})
        else:
            result = self._try_execute(db_path, sql, as_geojson)
            if "error" not in result:
                sql_history.append({"sql": result["sql_used"], "status": "ok", "error": None})
                return json.dumps({
                    "result": {"columns": result["columns"], "rows": result["rows"]},
                    "sql_history": sql_history,
                    "notes": "初回実行で成功"
                }, ensure_ascii=False)
            else:
                sql_history.append({"sql": result["sql"], "status": "error", "error": result["error"]})

        # リトライ
        last_error = sql_history[-1]["error"]
        last_sql = sql_history[-1]["sql"]
        for _ in range(retries):
            fix = self._fix_prompt(relation, schema, user_prompt, last_sql, last_error, max_rows)
            sql = self._extract_sql_only(self._ensure_llm().invoke(fix).content)
            if not _is_readonly(sql):
                sql_history.append({"sql": sql, "status": "error", "error": "forbidden keyword"})
                last_error, last_sql = "forbidden keyword", sql
                continue
            result = self._try_execute(db_path, sql, as_geojson)
            if "error" in result:
                sql_history.append({"sql": result["sql"], "status": "error", "error": result["error"]})
                last_error, last_sql = result["error"], result["sql"]
            else:
                sql_history.append({"sql": result["sql_used"], "status": "ok", "error": None})
                return json.dumps({
                    "result": {"columns": result["columns"], "rows": result["rows"]},
                    "sql_history": sql_history,
                    "notes": "自己修正により成功"
                }, ensure_ascii=False)

        return json.dumps({"error": "failed after retries", "sql_history": sql_history}, ensure_ascii=False)


# =============================== サンプル実行 ===============================
if __name__ == "__main__":
    # APIキー確認（dotenvがあれば読み込む）
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    if os.getenv("OPENAI_API_KEY") is None and os.getenv("OPEN_AI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = os.getenv("OPEN_AI_API_KEY")

    # 1) まずはデータを読み込む（例: GeoJSON を view 化）
    loader = LoadSpatialDataset()
    ctx_raw = loader.run(json.dumps({
        "source_type": "duckdb",          # "parquet" / "geoparquet" / "duckdb" も可
        "path": "./CityGMLData/plateau_buildings_osaka_duckdb.duckdb",    # 要置き換え
        "db_path": "geo.duckdb",
        "relation": "buildings",
        "mode": "view",                   # or "table"
        "srid": 4326,
        "add_bbox_columns": True,
    }))
    print("[Load]", ctx_raw)

    ctx = json.loads(ctx_raw)
    if "error" in ctx:
        raise SystemExit(ctx)

    # 2) 自然文 → SQL → 実行（自己修正つき）
    smart = RunSQLSmart()
    run_raw = smart.run(json.dumps({
        "db_path": ctx["db_path"],
        "relation": ctx["relation"],
        "user_prompt": "detailedUsage 別に件数を集計して多い順に",
        "max_rows": 200,
        "retries": 2,
        "as_geojson": False,
    }))
    print("[Smart]", run_raw)

    run = json.loads(run_raw)
    if "result" in run:
        # 3) 結果を自然言語で整形
        composer = ComposeAnswer()
        answer = composer.run(json.dumps({
            "user_prompt": "detailed Usage別に件数を集計して",
            "result": run["result"],
            "style": "summary",
        }))
        print("[Answer]", answer)
    else:
        print("[Error]", run)