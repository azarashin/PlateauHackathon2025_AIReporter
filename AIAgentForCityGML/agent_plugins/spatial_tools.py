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
import json, os, pathlib, re, ast
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
    def __init__(self, gml_dirs: list[str] | None):
        super().__init__(
            name="LoadSpatialDataset",
            func=self._run,
            description="GeoJSON/Parquet/.duckdb を読み込み、DuckDB 上に view/table として公開する。"
        )

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
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
    def __init__(self, gml_dirs: list[str] | None, llm: ChatOpenAI | None):
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
        try:
            p = json.loads(expression)
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
        db_path = p.get("db_path", "geo.duckdb")
        relation = p.get("relation", "buildings")
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        user_prompt = p.get("user_prompt")

        # 別形式（columns/group_by/limit 指定）の補助: user_prompt を合成
        if not user_prompt:
            cols = p.get("columns")
            grp = p.get("group_by")
            lim = p.get("limit", max_rows)
            if cols or grp:
                cols_str = ", ".join(cols) if isinstance(cols, list) else str(cols)
                grp_str = ", ".join(grp) if isinstance(grp, list) else str(grp)
                user_prompt = (
                    f"テーブル {relation} から {grp_str} ごとに {cols_str} を出力してください。"
                    f"{grp_str} で GROUP BY し、必要なら count の多い順に並べ、LIMIT {int(lim)} 以下にしてください。"
                )
            else:
                return json.dumps({"error": "missing user_prompt"}, ensure_ascii=False)

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
    def __init__(self, gml_dirs: list[str] | None):
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
    def __init__(self, gml_dirs: Optional[List[str]] = None, llm: Optional[ChatOpenAI] = None):
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
        # 既に最終レポートJSON（abstract, sections 等）の場合はパススルー
        try:
            p = json.loads(expression)
            if isinstance(p, dict) and ("abstract" in p and "sections" in p):
                return json.dumps(p, ensure_ascii=False)
        except Exception:
            return json.dumps({"error": "invalid expression"}, ensure_ascii=False)

        user_prompt = p.get("user_prompt", "")
        result = p.get("result")
        style = p.get("style", "summary")
        if result is None:
            # result がない場合は与えられた本文/テキストをそのまま返すか、簡易JSONで返す
            if "text" in p:
                return json.dumps({"abstract": p.get("text"), "sections": [{"title": "概要", "content": [{"type": "text", "content": p.get("text")}]}]}, ensure_ascii=False)
            return json.dumps({"error": "missing result"}, ensure_ascii=False)

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
    def __init__(self, gml_dirs: Optional[List[str]] = None, llm: Optional[ChatOpenAI] = None):
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
        # 2モード対応:
        # A) 提案+実行: {db_path, relation, user_prompt, ...}
        # B) 直接実行:   {sql, db_path?, as_geojson?}
        try:
            p = json.loads(expression)
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                return json.dumps({"error": "invalid expression"}, ensure_ascii=False)

        # 直接実行モード
        if isinstance(p, dict) and "sql" in p and p.get("user_prompt") is None and p.get("relation") is None:
            db_path = p.get("db_path", "geo.duckdb")
            as_geojson = bool(p.get("as_geojson", False))
            sql = str(p.get("sql", "")).strip()
            if not sql:
                return json.dumps({"error": "missing sql"}, ensure_ascii=False)
            result = self._try_execute(db_path, sql, as_geojson)
            if "error" in result:
                return json.dumps({
                    "error": result["error"],
                    "sql_history": [{"sql": result.get("sql", sql), "status": "error", "error": result["error"]}],
                    "notes": "direct sql error"
                }, ensure_ascii=False)
            return json.dumps({
                "result": {"columns": result["columns"], "rows": result["rows"]},
                "sql_history": [{"sql": result["sql_used"], "status": "ok", "error": None}],
                "notes": "direct sql"
            }, ensure_ascii=False)

        # 提案+実行モード
        db_path = p.get("db_path", "geo.duckdb")
        relation = p.get("relation", "buildings")
        user_prompt = p.get("user_prompt") or ""
        if not user_prompt:
            return json.dumps({"error": "missing user_prompt"}, ensure_ascii=False)
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


# =============================== ⑤ 災害計算ユーティリティ実行 ===============================
class RunHazardUtility(Tool):
    """
    小さな災害情報計算ユーティリティを選んで、RunSQLSmart でSQL案出し+実行する。

    入力JSON:
      {
        "db_path":"geo.duckdb",      # 必須
        "relation":"buildings",      # 必須
        "utility":"total_buildings|flood_height_and_river_risk|flood_depth_ge|summary_by_disaster_category", # 必須
        "params": { ... },             # 任意（ユーティリティ固有）
        "max_rows": 500,               # 任意
        "retries": 2,                  # 任意
        "as_geojson": false            # 任意
      }
    返り値(JSON): RunSQLSmartの返却そのまま（result, sql_history など）に utility と prompt を付与
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(
            name="RunHazardUtility",
            func=self._run,
            description="災害情報ユーティリティ（集計）を RunSQLSmart で実行する。"
        )

    def _build_prompt(self, relation: str, utility: str, params: Dict[str, Any]) -> str:
        # 可能な限り列名や出力名を固定化して、後段のパースを安定化
        if utility == "total_buildings":
            return (
                f"テーブル {relation} の全件数を1行で返してください。"
                "COUNT(*) を total という列名で出力します。"
            )
        if utility == "flood_height_and_river_risk":
            min_h = float(params.get("min_height", 3.0))
            return (
                "次の条件に合致する行の件数を1行で返してください。"
                f" measuredHeight を DOUBLE にキャストして {min_h} 以上、かつ"
                " disaster_risk_1_disaster_category, disaster_risk_2_disaster_category, disaster_risk_3_disaster_category"
                " のいずれかに '河川氾濫' を含む。存在する列のみ使用してください。"
                " 出力は COUNT(*) を cnt という列名で返してください。"
            )
        if utility == "flood_depth_ge":
            thr = float(params.get("threshold", 1.0))
            return (
                f"max_flood_depth を DOUBLE にキャストして {thr} 以上の行の件数を1行で返してください。"
                "出力は COUNT(*) を cnt という列名で返してください。"
            )
        if utility == "summary_by_disaster_category":
            return (
                "次の列のうち存在するものだけを使って、UNION ALL で1列(disaster_category)にまとめ、"
                "カテゴリ別の件数を集計してください。列: disaster_risk_1_disaster_category,"
                " disaster_risk_2_disaster_category, disaster_risk_3_disaster_category。"
                "出力は2列: disaster_category, cnt。件数の多い順に並べてください。"
            )
        raise ValueError("unknown utility")

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
        db_path = p["db_path"]
        relation = p["relation"]
        utility = p["utility"]
        params = p.get("params", {})
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        retries = int(p.get("retries", 2))
        as_geojson = bool(p.get("as_geojson", False))

        user_prompt = self._build_prompt(relation, utility, params)
        # RunSQLSmart は提案+実行を行う
        smart = RunSQLSmart([])  # gml_dirs は未使用のため空
        smart_out_raw = smart.run(json.dumps({
            "db_path": db_path,
            "relation": relation,
            "user_prompt": user_prompt,
            "max_rows": max_rows,
            "retries": retries,
            "as_geojson": as_geojson,
        }))
        out = json.loads(smart_out_raw)
        out["utility"] = utility
        out["prompt"] = user_prompt
        return json.dumps(out, ensure_ascii=False)


# =============================== 補助: ルータークラス群 ===============================
class HazardAggregationRouter:
    HAZARD_KWS = [
        "災害", "ハザード", "洪水", "浸水", "河川氾濫", "津波", "地震", "土砂", "土砂災害", "高潮", "液状化",
        "hazard", "flood", "inundation", "tsunami", "earthquake", "landslide", "liquefaction", "storm surge"
    ]

    @classmethod
    def is_match(cls, text: str) -> bool:
        t = text.lower()
        return any(k in text or k in t for k in cls.HAZARD_KWS)

    @classmethod
    def pick_utility(cls, text: str) -> tuple[str, Dict[str, Any]]:
        low = text.lower()
        params: Dict[str, Any] = {}
        # 高さ×河川氾濫
        if ("河川氾濫" in text) or ("river" in low and "flood" in low):
            m = re.search(r"(\d+(?:\.\d+)?)\s*m", text)
            params["min_height"] = float(m.group(1)) if m else 3.0
            return "flood_height_and_river_risk", params
        # 浸水深しきい値
        if ("浸水" in text) or ("inundation" in low) or ("flood" in low):
            m = re.search(r"(\d+(?:\.\d+)?)\s*m", text)
            params["threshold"] = float(m.group(1)) if m else 1.0
            return "flood_depth_ge", params
        # 災害種別別
        if ("災害種別" in text) or ("カテゴリ" in text) or ("category" in low):
            return "summary_by_disaster_category", params
        return "total_buildings", params


class StatisticalAggregationRouter:
    STAT_KWS = [
        "別", "件数", "集計", "多い順", "上位", "ランキング", "平均", "中央値", "分布", "グラフ",
        "group by", "count", "avg", "median", "distribution"
    ]

    @classmethod
    def is_match(cls, text: str) -> bool:
        t = text.lower()
        return any(k in text or k in t for k in cls.STAT_KWS)

    @classmethod
    def pick_group_column(cls, text: str, columns: List[str]) -> str:
        # ユーザ文に現れる列名を優先的に採用。なければ detailedUsage をデフォルトに。
        low = text.lower()
        for c in columns:
            if c and (c in text or c.lower() in low):
                return c
        return "detailedUsage" if "detailedUsage" in columns else (columns[0] if columns else "")


# =============================== ⑥ 統計ユーティリティ実行 ===============================
class RunStatUtility(Tool):
    """
    統計系の典型操作（カテゴリ別件数など）を RunSQLSmart で案出し+実行。

    入力JSON:
      {
        "db_path":"geo.duckdb",
        "relation":"buildings",
        "user_prompt":"...",    # ユーザの自然文
        "column":"detailedUsage" # 任意: group対象列（指定なければ推定）
      }
    返り値: RunSQLSmart の出力に utility と params と prompt を付与
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(
            name="RunStatUtility",
            func=self._run,
            description="統計ユーティリティ（カテゴリ別件数など）を RunSQLSmart で実行する。"
        )

    def _build_prompt(self, relation: str, column: str, base_user_prompt: str) -> str:
        # 基本はカテゴリ別件数（多い順）。ユーザ要望を補助的に含める。
        return (
            f"テーブル {relation} から {column} ごとの件数を集計してください。"
            f"SELECT {column} AS {column}, COUNT(*) AS count という列名で出力し、"
            f"{column} で GROUP BY し、count の多い順に並べ替えてください。"
            "ユーザ要望: " + base_user_prompt
        )

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception as e:
            return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
        db_path = p["db_path"]
        relation = p["relation"]
        user_prompt = p.get("user_prompt", "")
        column_hint = p.get("column")
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        retries = int(p.get("retries", 2))

        # 列の推定
        con = _connect(db_path)
        meta = _relation_preview(con, relation)
        con.close()
        columns = [c["name"] for c in meta.get("columns", [])]
        column = column_hint or StatisticalAggregationRouter.pick_group_column(user_prompt, columns)
        if not column:
            # 列が取れない場合は素直に RunSQLSmart に委譲
            smart = RunSQLSmart([])
            raw = smart.run(json.dumps({
                "db_path": db_path,
                "relation": relation,
                "user_prompt": user_prompt,
                "max_rows": max_rows,
                "retries": retries,
                "as_geojson": False
            }))
            out = json.loads(raw)
            out["utility"] = "stat_fallback"
            out["params"] = {"column": None}
            out["prompt"] = user_prompt
            return json.dumps(out, ensure_ascii=False)

        stat_prompt = self._build_prompt(relation, column, user_prompt)
        smart = RunSQLSmart([])
        raw = smart.run(json.dumps({
            "db_path": db_path,
            "relation": relation,
            "user_prompt": stat_prompt,
            "max_rows": max_rows,
            "retries": retries,
            "as_geojson": False
        }))
        out = json.loads(raw)
        out["utility"] = "group_count_by_column"
        out["params"] = {"column": column}
        out["prompt"] = stat_prompt
        return json.dumps(out, ensure_ascii=False)


# =============================== ⑦ 住民説明レポート生成 ===============================
class GenerateResidentReport(Tool):
    """
    入力JSON:
      {
        "db_path":"geo.duckdb",     # 必須
        "relation":"buildings",     # 必須 (LoadSpatialDataset で公開済みのビュー/テーブル)
        "area_name":"大阪市内"         # 任意（表示用）
      }
    返り値: 日本語のテキスト（住民説明用の簡潔な資料）
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(
            name="GenerateResidentReport",
            func=self._run,
            description="建物データから件数・洪水関連指標を集計し、住民説明用テキストを生成する。"
        )

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception:
            return "入力JSONが不正です。{\"db_path\":..., \"relation\":...} の形式で指定してください。"

        db_path = p.get("db_path")
        relation = p.get("relation")
        area_name = p.get("area_name", "大阪市内")
        if not db_path or not relation:
            return "db_path と relation を指定してください。"
        # RunHazardUtility を使って各指標を取得
        runner = RunHazardUtility()

        # 1) 総数
        total = 0
        r_total = json.loads(runner.run(json.dumps({
            "db_path": db_path, "relation": relation, "utility": "total_buildings", "max_rows": 1
        })))
        if "result" in r_total and r_total["result"].get("rows"):
            try:
                total = int(r_total["result"]["rows"][0][0])
            except Exception:
                total = 0

        # 2) 3m以上×河川氾濫
        flood_3m = 0
        r_f3 = json.loads(runner.run(json.dumps({
            "db_path": db_path, "relation": relation, "utility": "flood_height_and_river_risk",
            "params": {"min_height": 3.0}, "max_rows": 1
        })))
        if "result" in r_f3 and r_f3["result"].get("rows"):
            try:
                flood_3m = int(r_f3["result"]["rows"][0][0])
            except Exception:
                flood_3m = 0

        # 3) 1m以上の浸水リスク
        flood_1m = 0
        r_f1 = json.loads(runner.run(json.dumps({
            "db_path": db_path, "relation": relation, "utility": "flood_depth_ge",
            "params": {"threshold": 1.0}, "max_rows": 1
        })))
        if "result" in r_f1 and r_f1["result"].get("rows"):
            try:
                flood_1m = int(r_f1["result"]["rows"][0][0])
            except Exception:
                flood_1m = 0

        # 4) 災害カテゴリ別集計
        summary_rows: List[List[Any]] = []
        r_sum = json.loads(runner.run(json.dumps({
            "db_path": db_path, "relation": relation, "utility": "summary_by_disaster_category",
            "max_rows": 500
        })))
        if "result" in r_sum and r_sum["result"].get("rows"):
            summary_rows = r_sum["result"]["rows"]

        # テキスト組み立て
        lines: List[str] = []
        lines.append(f"本地区（{area_name}）には、合計{int(total):,}棟の建物データが登録されています。")
        lines.append(
            f"このうち、3m以上の高さがあり、河川氾濫リスクが想定される建物は{int(flood_3m):,}棟です。"
        )
        lines.append(f"また、1m以上の浸水リスクがある建物は{int(flood_1m):,}棟存在します。")
        if summary_rows:
            lines.append("災害種別ごとの建物件数は以下の通りです：")
            for row in summary_rows:
                if not row:
                    continue
                cat = row[0] if len(row) > 0 else None
                cnt = row[1] if len(row) > 1 else None
                cat_disp = cat if cat is not None else "不明"
                try:
                    cnt_int = int(cnt)
                except Exception:
                    # 2列目が見つからない/数値でない場合でも崩れないよう防御
                    cnt_int = 0
                lines.append(f"  - {cat_disp}: {cnt_int:,}棟")
        lines.append(
            "これらの情報は、立地適正化計画や防災対策の検討、住民の皆様への説明資料としてご活用いただけます。"
        )
        return "\n".join(lines)


# =============================== ⑦ インテントで自動振り分け ===============================
class OrchestrateQuery(Tool):
    """
    ユーザの自然文を見て、災害関連なら RunHazardUtility を、そうでなければ RunSQLSmart を呼ぶルーター。

    入力JSON:
      {
        "db_path":"geo.duckdb",     # 必須
        "relation":"buildings",     # 必須
        "user_prompt":"...",        # 必須
        "max_rows":500, "retries":2, "as_geojson":false  # 任意
      }
    返り値(JSON): 下流の返却（result/sql_history/notes）に route と utility を付与
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(
            name="OrchestrateQuery",
            func=self._run,
            description="自然文の意図に応じて RunHazardUtility か RunSQLSmart を実行する。"
        )

    def _run(self, expression: str) -> str:
        # 柔軟に入力を受け付ける（Agent からの呼び出しでキーが異なる場合があるため）
        try:
            p = json.loads(expression)
        except Exception:
            # 非JSONはそのまま user_prompt として扱う
            p = {"user_prompt": str(expression)}
        db_path = p.get("db_path", "geo.duckdb")
        relation = p.get("relation", "buildings")
        user_prompt = p.get("user_prompt") or p.get("prompt") or p.get("query") or p.get("text") or ""
        if not user_prompt:
            return json.dumps({"error": "missing user_prompt"}, ensure_ascii=False)
        max_rows = int(p.get("max_rows", DEFAULT_LIMIT))
        retries = int(p.get("retries", 2))
        as_geojson = bool(p.get("as_geojson", False))

        if HazardAggregationRouter.is_match(user_prompt):
            utility, params = HazardAggregationRouter.pick_utility(user_prompt)
            runner = RunHazardUtility()
            out_raw = runner.run(json.dumps({
                "db_path": db_path,
                "relation": relation,
                "utility": utility,
                "params": params,
                "max_rows": max_rows,
                "retries": retries,
                "as_geojson": as_geojson,
            }))
            out = json.loads(out_raw)
            out["route"] = "hazard"
            return json.dumps(out, ensure_ascii=False)
        elif StatisticalAggregationRouter.is_match(user_prompt):
            stat = RunStatUtility()
            out_raw = stat.run(json.dumps({
                "db_path": db_path,
                "relation": relation,
                "user_prompt": user_prompt,
                "max_rows": max_rows,
                "retries": retries
            }))
            out = json.loads(out_raw)
            out["route"] = "statistical"
            return json.dumps(out, ensure_ascii=False)
        else:
            smart = RunSQLSmart([])
            out_raw = smart.run(json.dumps({
                "db_path": db_path,
                "relation": relation,
                "user_prompt": user_prompt,
                "max_rows": max_rows,
                "retries": retries,
                "as_geojson": as_geojson,
            }))
            out = json.loads(out_raw)
            out["route"] = "general"
            return json.dumps(out, ensure_ascii=False)


# =============================== ⑧ ハザード専用パイプラインツール群 ===============================
DEFAULT_HAZARD_DATASET = {
    "source_type": "duckdb",
    "path": "./CityGMLData/plateau_buildings_osaka_duckdb.duckdb",
    "db_path": "geo.duckdb",
    "relation": "buildings",
    "mode": "view",
    "srid": 4326,
    "add_bbox_columns": True
}


class HazardLoadData(Tool):
    """
    ハザード分析用データのロード専用ツール。

    入力JSON（省略可）:
      { "dataset": { LoadSpatialDataset と同等の引数 }, "merge": true }
    省略/空なら既定(DEFAULT_HAZARD_DATASET)を使用。merge=true の場合は既定に上書きマージ。

    返り値: LoadSpatialDataset の返却(JSON)
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(name="HazardLoadData", func=self._run, description="ハザード用データを既定/指定でロードする。")

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression) if expression else {}
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                p = {}
        ds = p.get("dataset", {}) or {}
        merge = bool(p.get("merge", True))
        payload = dict(DEFAULT_HAZARD_DATASET)
        if merge and isinstance(ds, dict):
            payload.update(ds)
        elif ds:
            payload = ds
        loader = LoadSpatialDataset()
        return loader.run(json.dumps(payload, ensure_ascii=False))


class HazardProposeAndRun(Tool):
    """
    ハザード系ユーティリティ or ハザード自然文を提案+実行。

    入力JSON:
      {
        "db_path":"geo.duckdb",
        "relation":"buildings",
        "utility":"flood_height_and_river_risk|flood_depth_ge|summary_by_disaster_category|total_buildings",  # 任意
        "params": { ... },     # 任意（ユーティリティ用）
        "user_prompt":"..."   # 任意（自然文; 指定時はルーターでユーティリティ推定）
      }
    返り値: RunHazardUtility or RunSQLSmart の結果(JSON)に route/utility/prompt を付与
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(name="HazardProposeAndRun", func=self._run, description="ハザード自然文またはユーティリティを実行。")

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
        db_path = p.get("db_path", "geo.duckdb")
        relation = p.get("relation", "buildings")
        utility = p.get("utility")
        params = p.get("params", {})
        user_prompt = p.get("user_prompt")

        if utility:
            runner = RunHazardUtility()
            raw = runner.run(json.dumps({
                "db_path": db_path, "relation": relation, "utility": utility, "params": params
            }, ensure_ascii=False))
            out = json.loads(raw)
            out["route"] = "hazard"
            return json.dumps(out, ensure_ascii=False)

        if user_prompt:
            # ルーターでユーティリティ推定
            util, pr = HazardAggregationRouter.pick_utility(user_prompt)
            pr.update(params or {})
            runner = RunHazardUtility()
            raw = runner.run(json.dumps({
                "db_path": db_path, "relation": relation, "utility": util, "params": pr
            }, ensure_ascii=False))
            out = json.loads(raw)
            out["route"] = "hazard"
            return json.dumps(out, ensure_ascii=False)

        return json.dumps({"error": "missing utility or user_prompt"}, ensure_ascii=False)


class HazardReport(Tool):
    """
    住民説明向けの簡易ハザードレポートを生成。
    入力JSON: { "db_path":"geo.duckdb", "relation":"buildings", "area_name":"大阪市内" }
    返り値: テキスト
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(name="HazardReport", func=self._run, description="ハザードの住民説明レポートを生成する。")

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression)
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                return json.dumps({"error": "invalid expression"}, ensure_ascii=False)
        rep = GenerateResidentReport()
        return rep.run(json.dumps(p, ensure_ascii=False))


class RunHazardPipeline(Tool):
    """
    ハザード専用パイプライン（一括実行）

    入力JSON:
      {
        "dataset": { LoadSpatialDataset と同等 },   # 任意
        "tasks": [                                   # 任意: ユーティリティまたはタスク指定
          "summary_by_disaster_category",
          {"utility":"flood_height_and_river_risk", "params":{"min_height":3.0}}
        ],
        "prompts": ["河川氾濫の建物件数"],             # 任意: 自然文での追加ハザード問合せ
        "generate_report": true,                     # 任意
        "area_name": "大阪市内"                        # 任意
      }
    返り値:
      {
        "load_ctx": {...},
        "steps": [ {"kind":"utility|prompt", "input":..., "output":...}, ...],
        "report": "..."  # generate_report=true の場合
      }
    """

    def __init__(self, gml_dirs: Optional[List[str]] = None):
        super().__init__(name="RunHazardPipeline", func=self._run, description="ハザードデータのロード→集計→レポートを一括実行。")

    def _run(self, expression: str) -> str:
        try:
            p = json.loads(expression) if expression else {}
        except Exception:
            try:
                p = ast.literal_eval(expression)
            except Exception:
                p = {}

        # 1) ロード
        loader = HazardLoadData()
        load_raw = loader.run(json.dumps({
            "dataset": p.get("dataset", {}),
            "merge": True
        }, ensure_ascii=False))
        load_ctx = json.loads(load_raw)
        if "error" in load_ctx:
            return json.dumps({"error": load_ctx["error"], "stage": "load"}, ensure_ascii=False)
        db_path = load_ctx.get("db_path", DEFAULT_HAZARD_DATASET["db_path"])  # type: ignore
        relation = load_ctx.get("relation", DEFAULT_HAZARD_DATASET["relation"])  # type: ignore

        # 2) タスク実行
        steps = []
        runner = HazardProposeAndRun()
        for t in (p.get("tasks") or []):
            if isinstance(t, str):
                raw = runner.run(json.dumps({"db_path": db_path, "relation": relation, "utility": t}, ensure_ascii=False))
                steps.append({"kind": "utility", "input": t, "output": json.loads(raw)})
            elif isinstance(t, dict):
                util = t.get("utility"); params = t.get("params", {})
                raw = runner.run(json.dumps({"db_path": db_path, "relation": relation, "utility": util, "params": params}, ensure_ascii=False))
                steps.append({"kind": "utility", "input": t, "output": json.loads(raw)})

        # 3) 自然文プロンプト実行
        for q in (p.get("prompts") or []):
            raw = runner.run(json.dumps({"db_path": db_path, "relation": relation, "user_prompt": q}, ensure_ascii=False))
            steps.append({"kind": "prompt", "input": q, "output": json.loads(raw)})

        # 4) レポート
        report_text = None
        if bool(p.get("generate_report", False)):
            rep = HazardReport()
            report_text = rep.run(json.dumps({"db_path": db_path, "relation": relation, "area_name": p.get("area_name", "大阪市内")}, ensure_ascii=False))

        return json.dumps({"load_ctx": load_ctx, "steps": steps, "report": report_text}, ensure_ascii=False)


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
    loader = LoadSpatialDataset([])
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

    # 2) 災害関連の問い合わせ（ルーター経由→hazard になる想定）
    router = OrchestrateQuery()
    oq_hazard_raw = router.run(json.dumps({
        "db_path": ctx["db_path"],
        "relation": ctx["relation"],
        "user_prompt": "3m以上の高さで河川氾濫リスクがある建物件数を出して",
        "max_rows": 5,
        "retries": 2,
        "as_geojson": False
    }))
    print("[OrchestrateQuery:H]", oq_hazard_raw)
    try:
        oqh = json.loads(oq_hazard_raw)
        print("route:", oqh.get("route"), ", utility:", oqh.get("utility"))
    except Exception:
        pass

    # 3) Orchestrate 実行後に、住民説明レポートを生成
    reporter = GenerateResidentReport()
    report_text = reporter.run(json.dumps({
        "db_path": ctx["db_path"],
        "relation": ctx["relation"],
        "area_name": "大阪市内"
    }))
    print("[ResidentReport]\n" + report_text)
