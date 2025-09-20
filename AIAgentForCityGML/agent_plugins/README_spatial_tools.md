### Test

【プロンプト：mainを以下のように設定した場合】
２）と３）のuser_promptが重要

```python
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
```


【コマンド】
```bash
uv run python ./AIAgentForCityGML/agent_plugins/spatial_tools.py
```

【応答】
```bash
[Load] {"db_path": "geo.duckdb", "relation": "buildings", "kind": "view", "rows": 616319}
[Smart] {"result": {"columns": ["detailedUsage", "count"], "rows": [["一戸建住宅", 239853], [null, 115122], ["長屋建住宅", 45356], ["共同住宅", 43483], ["店舗併用住宅", 40598], ["事 務所", 22225], ["店舗併用共同住宅", 14512], ["車庫", 11179], ["都市型工業施設", 10707], [" その他の工業施設", 9290], ["工場併用住宅", 8084], ["その他の施設", 7796], ["流通施設", 7431], ["学校", 5558], ["小売販売店", 4662], ["一般飲食店", 3334], ["寺院", 2695], ["医療施設", 1726], ["福祉施設", 1550], ["サービス施設：一般", 1542], ["供給施設", 1205], ["集会施設", 1195], ["交通施設", 1081], ["工務店", 1031], ["神社", 1030], ["その他の宗教施設", 901], [" 建替え中施設", 872], ["その他の飲食店", 867], ["その他の教育施設", 844], ["大型小売店舗", 819], ["サービス施設：自動車修理工場", 745], ["遊興施設：パチンコ店等", 736], ["金融、保険", 729], ["処理施設", 685], ["自治体行政施設", 654], ["ガソリンスタンド", 532], ["団体", 525], ["保安施設", 517], ["専門的業務施設", 449], ["ホテル", 426], ["卸売販売施設", 385], ["遊 興施設：キャバレー等", 355], ["その他の宿泊施設", 298], ["郵便局", 258], ["文化施設", 255], ["スポーツ娯楽施設", 248], ["保管施設", 243], ["ラブホテル", 228], ["キリスト教会", 202], ["興業施設", 200], ["大学、短大", 198], ["国家施設", 197], ["運動施設", 131], ["農林・漁業 施設", 107], ["展示場", 106], ["遊興施設：性風俗店等", 82], ["通信施設", 76], ["研究施設", 59], ["小売市場", 56], ["保健施設", 55], ["報道施設", 34]]}, "sql_history": [{"sql": "SELECT detailedUsage, COUNT(*) AS count \nFROM buildings \nGROUP BY detailedUsage \nORDER BY count DESC \nLIMIT 500;", "status": "ok", "error": null}], "notes": "初回実行で成功"}
[Answer] 以下は、detailedUsage別の件数集計結果です。合計61件のデータがあり、先頭行の例は「 一戸建住宅」で239,853件となっています。

| detailedUsage   | count   |
|------------------|---------|
| 一戸建住宅       | 239,853 |
| ...              | ...     |
| (他のデータ)    | ...     |

このように、各detailedUsageに対する件数を集計した結果を示しています。詳細なデータについては、必要に応じてお知らせください。
```