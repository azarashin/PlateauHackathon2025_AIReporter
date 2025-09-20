### Test

```bash
uv run python -c "import json; from AIAgentForCityGML.agent_plugins.e_stat import EStatTool; t=EStatTool(); print(t.run(json.dumps({'stats_id':'0003445078','params':{'cdTime':'2020000000','cdCat01':'0'},'area':{'names':[' 大阪市中央区'],'index_json':'./CityGMLData/city_codes_osaka.json'}})))"
```

