from pathlib import Path
import sys
import urllib.request
import xml.etree.ElementTree as ET

class GMLIDMean:
    def __init__(self, url: str) -> dict[str, str]:
        """
        指定したURLのGML辞書XMLを取得し、
        gml:name をキー、gml:description を値とする辞書を返します。
        """
        # --- 1. XMLファイルを取得 ---
        if url[:8] == 'https://':
            with urllib.request.urlopen(url) as response:
                xml_data = response.read()
        else:
            xml_data = open(url, 'r', encoding='utf-8').read()

        # --- 2. パース ---
        root = ET.fromstring(xml_data)

        # gml 名前空間を設定
        ns = {"gml": "http://www.opengis.net/gml"}

        # --- 3. gml:dictionaryEntry 内の gml:Definition を走査 ---
        result: dict[str, str] = {}
        for definition in root.findall("gml:dictionaryEntry/gml:Definition", ns):
            name = definition.findtext("gml:name", namespaces=ns)
            desc = definition.findtext("gml:description", namespaces=ns)
            if name and desc:
                result[name] = desc

        self.map = result
    
    def get_mean(self, value: str) -> str:
        if value in self.map:
            return self.map[value]
        return None
    
    def get_names(self):
        return list(self.map.keys())

class GMLAttribMapper: 
    def __init__(self, base_dir: str = ''):
        target_dir = Path(f'{base_dir}/codelists')
        if not base_dir.is_dir():
            print(f"ディレクトリが見つかりません: {target_dir}")
            return

        xml_files = list(target_dir.rglob("*.xml"))
        if not xml_files:
            print("*.xml ファイルが見つかりません。")
            return

        self.map = {}
        self.map_attribute = {}
        for xml_file in xml_files:
            feature = xml_file.name.split('.')[0].split('_')[0]
            attribute = xml_file.name.split('.')[0].split('_')[1]
            self.map[(feature, attribute)] = GMLIDMean(str(xml_file.resolve()))
            if not attribute in self.map_attribute:
                self.map_attribute[attribute] = []
            self.map_attribute[attribute].append(self.map[(feature, attribute)])
    
    def get_mean(self, feature: str, attribute: str): 
        if (feature, attribute) in self.map:
            return self.map[(feature, attribute)]
        # これらの属性に対する判定ロジックが不明なので暫定的に記述
        if attribute in ('prefecture', 'city'):
            return self.map[('Common', 'localPublicAuthorities')]
        
        '''
        <uro:buildingDisasterRiskAttribute>
            <uro:BuildingLandSlideRiskAttribute>
                <uro:description codeSpace="../../codelists/LandSlideRiskAttribute_description.xml">2</uro:description>
                <uro:areaType codeSpace="../../codelists/LandSlideRiskAttribute_areaType.xml">2</uro:areaType>
            </uro:BuildingLandSlideRiskAttribute>
        </uro:buildingDisasterRiskAttribute>

        上記のような属性descriptionは

        buildingDisasterRiskAttribute|BuildingLandSlideRiskAttribute|description

        と表記している（前処理スクリプトcity_gml2meta.py の挙動）。
        この場合は
        BuildingLandSlideRiskAttributeとdescription とをそれぞれfeature, attribute としてチェックする。
        それでも見つからなかったら、
        BuildingLandSlideRiskAttribute = Building + LandSlideRiskAttribute
        と、解釈できないか試みる。
        （この２つをくっつける解釈で正しいのか？正しい解釈の仕方は別途調査が必要。）

        '''
        may_attribs = attribute.split('|')
        if len(may_attribs) >= 2:
            may_feature = may_attribs[-2]
            may_attribute = may_attribs[-1]
            if (may_feature, may_attribute) in self.map:
                return self.map[(may_feature, may_attribute)]
            if feature == may_feature[:len(feature)]:
                may_feature = may_feature[len(feature):]
                if (may_feature, may_attribute) in self.map:
                    return self.map[(may_feature, may_attribute)]

        if attribute in self.map_attribute:
            return self.map_attribute[attribute][0]
        return None



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("使い方: python gml_attrib_mapper.py <ディレクトリパス>")
        exit(1)

    base_dir = Path(sys.argv[1])

    usage = GMLIDMean('https://www.geospatial.jp/iur/codelists/3.0/Building_usage.xml')
    print(usage.get_mean('401'))
    print(usage.get_mean('402'))
    gmlam = GMLAttribMapper(base_dir)
    count = 0
    key = {}
    for feature, attrib in gmlam.map:
        print(f'{count}. {feature}, {attrib}')
        key[count] = (feature, attrib)
        count += 1

    input_number = input('number: ')
    try:
        number = int(input_number)
        feature, attrib = key[number]
    except:
        feature, attrib = input_number.split(' ')
    print(f'feature: {feature}, attribute: {attrib}')
    mean = gmlam.get_mean(feature, attrib)
    if mean is None:
        print('指定されたfeature とattribute の組み合わせが見つからない')
        exit()
    names = mean.get_names()

    for i in range(len(names)):
        print(f'{i}. {names[i]}')
    number_for_name = int(input('number: '))
    name = names[number_for_name]
    mean = mean.get_mean(name)
    print(f'{names[number_for_name]} -> {mean}')


