import json
from pathlib import Path
import sys

from gml_attrib_mapper import GMLAttribMapper, GMLIDMean
from gml_attribute import CityGMLAttribute

class GMLStat:
    def __init__(self, path: str, city_gml_attribute: CityGMLAttribute, mapper: GMLAttribMapper):
        self._city_gml_attribute = city_gml_attribute
        self._mapper = mapper
        source = open(path, 'r', encoding='utf-8')
        self._json = json.load(source)
        self._numeric_attrib = {}
        self._string_attrib = {}

        for i in range(self.get_layer_count()):
            numeric = {}
            for attrib_name in  self._json['layers'][i]['numeric_field_stats']:
                numeric[attrib_name] = NumericStats(self._json['layers'][i]['numeric_field_stats'][attrib_name])
            self._numeric_attrib[i] = numeric
    
    '''
    元になった.gml ファイルの場所
    '''
    def get_source(self):
        return self._json['source']
    
    def get_driver(self):
        return self._json['driver']

    '''
    レイヤ数
    '''
    def get_layer_count(self):
        return len(self._json['layers'])

    '''
    レイヤ名
    '''
    def get_layer_name(self, layer_index: int):
        return self._json['layers'][layer_index]['name']

    '''
    数値属性の名前一覧
    '''
    def get_numeric_attribute_names(self, layer_index: int):
        return list(self._numeric_attrib[layer_index].keys())

    '''
    数値属性
    '''
    def get_numeric_attribute(self, layer_index: int, attrib_name: str):
        return self._numeric_attrib[layer_index][attrib_name]

    '''
    文字列属性の名前一覧
    '''
    def get_string_attribute_names(self, layer_index: int):
        return list(self._json['layers'][layer_index]['string_field_frequencies'].keys())

    '''
    文字列属性
    '''
    def get_string_attribute(self, layer_index: int, attrib_name: str):
        return self._json['layers'][layer_index]['string_field_frequencies'][attrib_name]


    def get_attribute_mean(self, layer_index: int, attrib_name: str):
        return self._city_gml_attribute.get_description(self.get_layer_name(layer_index), attrib_name)

    '''
    文字列属性(属性値に意味があれば意味表現で置き換える)
    複数属性あれば、それぞれの属性ごとに発生したものとしてカウントする
    例：
    [1]: 2
    [1, 2]: 3
    [2, 1]: 4
    [2]: 1
    ^>
    1: 2 + 3 + 4 = 9
    2: 3 + 4 + 1 = 8

    '''
    def get_string_attribute_mean(self, layer_index: int, attrib_name: str):
        feature = self.get_layer_name(layer_index)
        original = self._json['layers'][layer_index]['string_field_frequencies'][attrib_name]
        mean_map = self._mapper.get_mean(feature, attrib_name)
        if mean_map is None:
            return original
        multi_map = {self._get_mean(mean_map, d):original[d] for d in original}
        ret = {}
        for key in multi_map:
            if type(key) is tuple:
                for single_key in key:
                    if not single_key in ret:
                        ret[single_key] = 0
                    ret[single_key] += multi_map[key]
            else:
                if not key in ret:
                    ret[key] = 0
                ret[key] += multi_map[key]
        return ret
    
    def _get_mean(self, mean_map: GMLIDMean, original: str):
        original = original.strip()
        if original[0] == '[' and original[-1] == ']':
            originals = original[1:-1].split(',')
            return tuple([self._get_mean(mean_map, d) for d in originals])
        result = mean_map.get_mean(original)
        if result is None:
            return original
        return result




class NumericStats:
    def __init__(self, json):
        self._json = json

    '''
    サンプル数
    '''
    def get_count(self) -> float:
        return self._json['count']

    '''
    最小値
    '''
    def get_min(self) -> float:
        return self._json['min']

    '''
    最大値
    '''
    def get_max(self) -> float:
        return self._json['max']

    '''
    算術平均
    '''
    def get_mean(self) -> float:
        return self._json['mean']
    
    '''
    ヒストグラム(区切りリスト, 区切り範囲毎のサンプル数)
    '''
    def get_histogram(self) -> tuple[list[float], list[float]]:
        return (self._json['histogram']['bin_edges'], self._json['histogram']['counts'])
    
    def __str__(self):
        return f'count: {self.get_count()}\n' + \
            f'min: {self.get_min()}\n' + \
            f'max: {self.get_max()}\n' + \
            f'mean: {self.get_mean()}\n' + \
            f'histogram: {self.get_histogram()}\n'

class GmlStatManager:
    def __init__(self, base_dir: str):
        gml_stats_files = list(base_dir.rglob("*.stat.json"))
        if not gml_stats_files:
            print("*.stat.json ファイルが見つかりません。")
            return
        
        mapper = GMLAttribMapper(base_dir)
        city_gml_attribute = CityGMLAttribute(base_dir)

        self.stat_list = [GMLStat(gml_stat, city_gml_attribute, mapper) for gml_stat in gml_stats_files]

    def show_menu(self):
        """コンソールメニューを表示してユーザ選択を受け取り、その統計を出力"""
        if not self.stat_list:
            return
        print("==== GML統計ファイル一覧 ====")
        for i, stat in enumerate(self.stat_list):
            print(f"{i}: {stat.get_source()}")

        while True:
            try:
                idx = int(input("表示したい番号を入力してください: "))
                if 0 <= idx < len(self.stat_list):
                    break
                else:
                    print("範囲外の番号です。")
            except ValueError:
                print("数字を入力してください。")

        # 選ばれた統計情報を表示（ここでは簡易表示）
        chosen = self.stat_list[idx]
        print(f"\n--- {chosen.get_source()} ---")
        for layer_idx in range(chosen.get_layer_count()):
            print(f"[Layer {layer_idx}] {chosen.get_layer_name(layer_idx)}")
            for name in chosen.get_numeric_attribute_names(layer_idx):
                print(f"  Numeric Attribute: {name}({chosen.get_attribute_mean(layer_idx, name)})")
                print(chosen.get_numeric_attribute(layer_idx, name))
            for name in chosen.get_string_attribute_names(layer_idx):
                print(f"  String Attribute: {name}({chosen.get_attribute_mean(layer_idx, name)})")
                print("   Frequencies:", chosen.get_string_attribute(layer_idx, name))
                print("   Frequencies(Mean):", chosen.get_string_attribute_mean(layer_idx, name))


def main():
    if len(sys.argv) < 2:
        print("使い方: python gml_stats_manager.py <ディレクトリパス>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)

    gsm = GmlStatManager(base_dir)
    gsm.show_menu()

if __name__ == "__main__":
    main()
