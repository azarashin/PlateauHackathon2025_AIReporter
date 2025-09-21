import csv
import logging
from io import StringIO
from pathlib import Path
import sys
import pandas as pd
import numpy as np

class CityGMLAttributeAttribute:
    """
    CityGMLの属性定義を表すクラス。
    タブ区切りの文字列1行を受け取り、各列を対応する属性に格納します。
    """

    def __init__(self, cols: list[str]):
        self._setup(cols)

    def _setup(self, cols: list[str]):

        # 必須列数チェック（不足分はNoneで補完）
        expected = 14
        if len(cols) < expected:
            cols += [None] * (expected - len(cols))

        (
            self.model_prefix,         # モデルの接頭辞
            self.feature_name,         # 地物名
            self.attr_or_role1,        # 属性名／関連役割名1
            self.attr_or_role2,        # 属性名／関連役割名2
            self.attr_or_role3,        # 属性名／関連役割名3
            self.attr_or_role4,        # 属性名／関連役割名4
            self.attribute_category,   # 主題属性、空間属性、関連役割の区分
            self.description,          # 説明
            self.creation_target,      # 作成対象
            self.additional_target,    # 追加対象
            self.code_extension,       # コード拡張
            self.remarks,              # 備考
            self.mandatory_flag,       # データ作成上必須
            self.data_source           # 想定されるデータソース
        ) = cols[:expected]
        self.cols = cols
        self.attribute = None
        if self.feature_name is not np.nan:
            self.attribute = self.feature_name
        if self.attr_or_role1 is not np.nan:
            self.attribute = self.attr_or_role1
        if self.attr_or_role2 is not np.nan:
            self.attribute = self.attr_or_role2
        if self.attr_or_role3 is not np.nan:
            self.attribute = self.attr_or_role3
        if self.attr_or_role4 is not np.nan:
            self.attribute = self.attr_or_role4

        self.parent = None
        self.children = []
        self.feature = None

    def get_attribute(self):
        return self.attribute
    
    def get_description(self):
        return self.description
    
    def get_key(self):
        return (self.get_root().feature_name.split(':')[-1], self.attribute.split(':')[-1])
    
    def get_root(self):
        if self.parent:
            return self.parent.get_root()
        return self

    def get_full_attribute(self):
        if self.parent:
            return f'{self.parent.get_full_attribute()}-{self.attribute}'
        return self.attribute

    def _split_tsv_preserve_quotes(self, line: str) -> list[str]:
        """
        タブ区切り文字列を分割する。
        "" で囲まれた部分は分割しない。
        タブが連続している場合は空文字をフィールドとして保持する。
        """
        # csv.reader に delimiter='\t' と quotechar='"' を設定
        reader = csv.reader(StringIO(line), delimiter='\t', quotechar='"')
        return next(reader)

    def __repr__(self):
        return (
            f"CityGMLAttributeAttribute("
            f"model_prefix={self.model_prefix!r}, "
            f"feature_name={self.feature_name!r}, "
            f"attr_or_role1={self.attr_or_role1!r}, "
            f"attr_or_role2={self.attr_or_role2!r}, "
            f"attr_or_role3={self.attr_or_role3!r}, "
            f"attr_or_role4={self.attr_or_role4!r}, "
            f"attribute_category={self.attribute_category!r}, "
            f"description={self.description!r}, "
            f"creation_target={self.creation_target!r}, "
            f"additional_target={self.additional_target!r}, "
            f"code_extension={self.code_extension!r}, "
            f"remarks={self.remarks!r}, "
            f"mandatory_flag={self.mandatory_flag!r}, "
            f"data_source={self.data_source!r})"
        )

class CityGMLAttribute:
    """
    CityGML のobjectlist_op.txt から属性一覧を抽出する。
    objectlist_op.txt はCityGML データに含まれる
    XXX_objectlist_op.xmlx
    をタブ区切りUTF-8 ファイルとして保存しなおしたものである。
    """
    def __init__(self, base_dir):
        path = self.find_xlsx_file(f'{base_dir}/specification')
        logging.info(f'base_dir: {base_dir}, xml-path: {path}')

        # Excelファイルの読み込み（1つ目のシート）
        df = pd.read_excel(path, header=[0, 1], sheet_name="A.3.1_取得項目一覧", engine="openpyxl")
        rows_as_cells: list[list[str]] = df.values.tolist()

        attr = [None, None, None, None, None]
        self.attributes = []
        self.tree = []
        for line in rows_as_cells:
            cgaa = CityGMLAttributeAttribute(line)
            if not cgaa.feature_name is np.nan:
                attr[0] = cgaa
                self.tree.append(cgaa)
            if not cgaa.attr_or_role1 is np.nan:
                attr[1] = cgaa
                cgaa.parent = attr[0]
                attr[0].children.append(cgaa)
                cgaa.feature = attr[0]
            if not cgaa.attr_or_role2 is np.nan:
                attr[2] = cgaa
                cgaa.parent = attr[1]
                attr[1].children.append(cgaa)
                cgaa.feature = attr[0]
            if not cgaa.attr_or_role3 is np.nan:
                attr[3] = cgaa
                cgaa.parent = attr[2]
                attr[2].children.append(cgaa)
                cgaa.feature = attr[0]
            if not cgaa.attr_or_role4 is np.nan:
                attr[4] = cgaa
                cgaa.parent = attr[3]
                attr[3].children.append(cgaa)
                cgaa.feature = attr[0]
            self.attributes.append(cgaa)
        self.map = {d.get_key():d for d in self.attributes}
    
    def get_description(self, layer_name: str, attribute: str):
        if (layer_name, attribute) in self.map:
            return self.map[(layer_name, attribute)].get_description()
    
    def find_xlsx_file(self, directory: str) -> Path | None:
        """directory直下にあるxlsxファイルを1件だけ取得する。
        見つからなければ None を返す。
        """
        d = Path(directory)

        if not d.is_dir():
            raise NotADirectoryError(f"指定ディレクトリが存在しません: {directory}")

        # ディレクトリ直下にある .xlsx ファイルを取得
        xlsx_files = list(d.glob("*.xlsx"))

        if not xlsx_files:
            print("xlsxファイルが見つかりませんでした。")
            return None

        if len(xlsx_files) > 1:
            print("複数のxlsxファイルが見つかりました。")
            return None

        # 1件だけあった場合、そのパスを返す
        return xlsx_files[0]

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python gml_attribute.py <ディレクトリパス>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)


    city_gml_attribute = CityGMLAttribute(base_dir)
    test_cases = [
        ('Building', 'usage'),
        ('WaterBody', 'name'),
        ('WaterBody', 'rank'),
        ('WaterBody', 'adminType'),
    ]
    for test_case in test_cases:
        layer_name = test_case[0]
        attribute = test_case[1]
        description = city_gml_attribute.get_description(layer_name, attribute)
        print(f'layer_name: {layer_name}, attribute: {attribute}, description: {description}')
    