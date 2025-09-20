# 導入準備

## GDAL の入手

https://github.com/cgohlke/geospatial-wheels/releases/tag/v2025.7.4

python のバージョンに注意
python3.13 なら下記を使用する。
gdal-3.11.1-cp313-cp313-win_amd64.whl

# スクリプト一覧

## city_gml2meta.py

ディレクトリを指定するとCityGML データからメタデータを抽出する。
抽出されたファイルのファイル名はXXX.gml.json になる。
このスクリプト単体で使用する。

## gml_stats_all.py

ディレクトリを指定するとCityGML データからメタデータを抽出し、
属性情報の統計情報をさらに抽出する。
抽出されたファイルのファイル名はXXX.gml.stat.json になる。
このスクリプト単体で使用する。

## gml_attribute.py

CityGML の属性情報を管理する。
メタデータから抽出した属性情報の意味（日本語表記）を取得するのに用いる。

使用例:

```py
>>> cga = CityGMLAttribute('40130_fukuoka-shi_city_2024_citygml_1_op')
>>> cga.map[('Building', 'usage')].get_description() 
'用途'
>>> cga.map[('Building', 'Building')].get_description()
'建築物'
```

## gml_attrib_mapper.py

codelists ディレクトリ配下の定義ファイルを元に、属性値からその説明文字列に変換する。
GmlStatManager が使用する。
