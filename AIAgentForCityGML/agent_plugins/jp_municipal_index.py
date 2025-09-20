# =============================
# 別ファイル: jp_municipal_index.py  (JSON版に統一)
# =============================
# 全国の市区町村コードと名称の対応を検索するユーティリティ（JSON専用）。
# - city_codes.json を読み込んで索引を構築する。
# - フォーマット（どちらか一方でOK）
#   A) 配列形式: [{"area_code":"01100","name":"札幌市","level":4,"parent_code":"01000","aliases":["札幌"]}, ...]
#   B) 連想配列: {"01100": {"name":"札幌市","level":4,"parent_code":"01000","aliases":["札幌"]}, ...}
#
# * level: 1=全国,2=都道府県,4=市,5=区 を推奨
# * aliases は任意（例: "中央区(札幌市)" など）

from dataclasses import dataclass
from typing import Dict, List, Optional
import json as _json
import os

@dataclass
class AreaRec:
    code: str
    name: str
    level: int
    parent: Optional[str] = None

class MunicipalIndex:
    def __init__(self):
        self.by_code: Dict[str, AreaRec] = {}
        self.by_name: Dict[str, List[str]] = {}

    @staticmethod
    def _norm(s: str) -> str:
        return (s or "").strip().replace("　", " ").replace(" ", "")

    def add(self, rec: AreaRec, aliases: Optional[List[str]] = None):
        self.by_code[rec.code] = rec
        names = [rec.name] + (aliases or [])
        for nm in names:
            self.by_name.setdefault(self._norm(nm), []).append(rec.code)
        # 区には「区名(市名)」の別名も追加
        if rec.parent and rec.level == 5:
            parent = self.by_code.get(rec.parent)
            if parent:
                alt = f"{rec.name}({parent.name})"
                self.by_name.setdefault(self._norm(alt), []).append(rec.code)

    def find_codes_by_name(self, name: str) -> List[str]:
        codes = self.by_name.get(self._norm(name), [])
        # 重複排除（順序保持）
        seen, out = set(), []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    def find_one_code(self, name: str) -> Optional[str]:
        arr = self.find_codes_by_name(name)
        return arr[0] if arr else None

    def load_json(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Municipal JSON not found: {path}")
        with open(path, encoding='utf-8') as f:
            obj = _json.load(f)
        if isinstance(obj, dict):
            for code, meta in obj.items():
                name = meta.get('name') or meta.get('city_name') or meta.get('ward_name') or meta.get('pref_name')
                level = int(meta.get('level') or 0)
                parent = meta.get('parent_code')
                aliases = meta.get('aliases') or []
                self.add(AreaRec(code=code, name=name, level=level, parent=parent), aliases=aliases)
        elif isinstance(obj, list):
            for row in obj:
                code = row.get('area_code') or row.get('code')
                if not code:
                    continue
                name = row.get('name') or row.get('city_name') or row.get('ward_name') or row.get('pref_name')
                level = int(row.get('level') or 0)
                parent = row.get('parent_code')
                aliases = row.get('aliases') or []
                self.add(AreaRec(code=code, name=name, level=level, parent=parent), aliases=aliases)
        else:
            raise ValueError('Unsupported JSON schema')

_cached_index: Optional[MunicipalIndex] = None

def get_index(json_path: str) -> MunicipalIndex:
    global _cached_index
    if _cached_index is not None:
        return _cached_index
    idx = MunicipalIndex()
    idx.load_json(json_path)
    _cached_index = idx
    return idx

def resolve_names_to_codes(names: List[str], json_path: str) -> List[str]:
    idx = get_index(json_path)
    out: List[str] = []
    for n in names:
        c = idx.find_one_code(str(n))
        if c:
            out.append(c)
    return out